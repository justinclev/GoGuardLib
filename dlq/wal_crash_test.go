//go:build unix

package dlq_test

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
)

var recordID = regexp.MustCompile(`^r\d+-\d{6}$`)

// crashOptions makes the child rotate and compact constantly so a kill is likely
// to land in the middle of either.
func crashOptions() dlq.WALOptions {
	return dlq.WALOptions{SegmentBytes: 4096, CompactMinBytes: 8 << 10, CompactRatio: 1}
}

// TestWALCrashChild is the workload the parent kills. It only runs when the
// parent starts it; on its own it is skipped.
//
// It reports on stdout, and the order matters. "A id" is printed AFTER Append
// returned, so the parent may rely on it: the store had acknowledged that record.
// For completing a record the parent needs the opposite: "P id" is printed BEFORE
// Ack is called (the ack may or may not have happened when the process died) and
// "D id" AFTER it returned (it certainly had). A record with "P" but no "D" may
// legitimately be present or absent.
func TestWALCrashChild(t *testing.T) {
	dir := os.Getenv("DLQ_CRASH_DIR")
	if dir == "" {
		t.Skip("only runs as a child of TestWALSurvivesKillNine")
	}
	round := os.Getenv("DLQ_CRASH_ROUND")
	s, err := dlq.OpenWAL(dir, crashOptions())
	if err != nil {
		fmt.Printf("OPENFAIL %v\n", err)
		os.Exit(3)
	}
	ctx := context.Background()
	for i := 0; ; i++ {
		id := fmt.Sprintf("r%s-%06d", round, i)
		if err := s.Append(ctx, dlq.Record{ID: id, Value: []byte(strings.Repeat("x", 50+i%40))}); err != nil {
			fmt.Printf("ERR append %v\n", err)
			os.Exit(4)
		}
		fmt.Printf("A %s\n", id)
		if i%3 == 2 {
			ls, err := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Hour})
			if err != nil {
				fmt.Printf("ERR lease %v\n", err)
				os.Exit(4)
			}
			for _, l := range ls {
				fmt.Printf("P %s\n", l.Record.ID)
				if err := s.Ack(ctx, l.Record.ID, l.Token); err != nil {
					fmt.Printf("ERR ack %v\n", err)
					os.Exit(4)
				}
				fmt.Printf("D %s\n", l.Record.ID)
			}
		}
	}
}

// Kill -9 the writer at random moments, over and over, on the same directory.
// After every kill: the store must open, every record whose Append was
// acknowledged and that was not completed must be present, and every record
// whose Ack was acknowledged must be gone.
func TestWALSurvivesKillNine(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns and kills processes")
	}
	dir := t.TempDir()
	appended := map[string]bool{}
	pending := map[string]bool{} // an Ack was started; it may or may not have completed
	done := map[string]bool{}    // an Ack returned
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	const rounds = 10
	for round := 0; round < rounds; round++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestWALCrashChild$", "-test.v")
		cmd.Env = append(os.Environ(), "DLQ_CRASH_DIR="+dir, "DLQ_CRASH_ROUND="+strconv.Itoa(round))
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}

		lines := make(chan string, 1<<16)
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				lines <- sc.Text()
			}
			close(lines)
		}()

		time.Sleep(time.Duration(150+rng.Intn(500)) * time.Millisecond)
		_ = cmd.Process.Kill() // SIGKILL: no deferred function, no flush, no Close
		_ = cmd.Wait()

		acks := 0
		for line := range lines {
			kind, id, ok := strings.Cut(line, " ")
			switch {
			case !ok:
			case kind == "OPENFAIL" || kind == "ERR":
				t.Fatalf("round %d: child reported %q", round, line)
			case !recordID.MatchString(id): // a line cut short by the kill
			case kind == "A":
				appended[id] = true
				acks++
			case kind == "P":
				pending[id] = true
			case kind == "D":
				done[id] = true
			}
		}
		if acks == 0 {
			t.Fatalf("round %d: child made no progress", round)
		}

		s, err := dlq.OpenWAL(dir, crashOptions())
		if err != nil {
			t.Fatalf("round %d: store did not recover after kill -9: %v", round, err)
		}
		present := map[string]bool{}
		for {
			ls, err := s.Lease(context.Background(), dlq.LeaseRequest{Max: 500, TTL: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			if len(ls) == 0 {
				break
			}
			for _, l := range ls {
				present[l.Record.ID] = true
			}
		}
		rep := s.Recovery()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}

		var lost, resurrected int
		for id := range appended {
			if !done[id] && !pending[id] && !present[id] {
				lost++
				t.Errorf("round %d: record %s was acknowledged but is gone", round, id)
			}
		}
		for id := range done {
			if present[id] {
				resurrected++
				t.Errorf("round %d: record %s was acknowledged as done but came back", round, id)
			}
		}
		if lost+resurrected > 0 {
			t.Fatalf("round %d: %d lost, %d resurrected (recovery %+v)", round, lost, resurrected, rep)
		}
		t.Logf("round %d: %d appends acked before kill, %d live after recovery, truncated %d bytes, snapshot LSN %d",
			round, acks, len(present), rep.TruncatedBytes, rep.SnapshotLSN)
	}
}
