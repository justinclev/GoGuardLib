package dlq

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

func sampleRecord() Record {
	return Record{
		ID: "rec-1", Seq: 42, State: Pending,
		Source:   Source{Kind: "kafka", Name: "orders", Partition: -1, Offset: 1 << 40},
		Key:      []byte("k"),
		Value:    nil,
		Headers:  []Header{{Key: "a", Value: []byte("1")}, {Key: "a", Value: nil}, {Key: "", Value: []byte{}}},
		OrderKey: "o", BlockedOn: "payments", Attempts: 7,
		FirstFailed: time.Date(2025, 3, 1, 10, 0, 0, 123456789, time.UTC),
		LastFailed:  time.Date(2025, 3, 1, 11, 0, 0, 0, time.UTC),
		NextAttempt: time.Time{},
		LastError:   "boom é ☃", Checkpoint: []byte{}, SchemaVersion: 1,
	}
}

func recordsEqual(a, b Record) bool {
	if !a.FirstFailed.Equal(b.FirstFailed) || !a.LastFailed.Equal(b.LastFailed) || !a.NextAttempt.Equal(b.NextAttempt) {
		return false
	}
	a.FirstFailed, a.LastFailed, a.NextAttempt = time.Time{}, time.Time{}, time.Time{}
	b.FirstFailed, b.LastFailed, b.NextAttempt = time.Time{}, time.Time{}, time.Time{}
	return reflect.DeepEqual(a, b)
}

func TestRecordRoundTripPreservesNilEmptyAndOrder(t *testing.T) {
	in := sampleRecord()
	var e encoder
	encodeRecord(&e, &in)
	d := decoder{b: e.b}
	out := decodeRecord(&d)
	if err := d.done(); err != nil {
		t.Fatal(err)
	}
	if !recordsEqual(in, out) {
		t.Fatalf("round trip differs:\n in  %+v\n out %+v", in, out)
	}
	if out.Value != nil || out.Checkpoint == nil || out.Headers[1].Value != nil || out.Headers[2].Value == nil {
		t.Fatal("nil versus empty was not preserved")
	}
}

func TestFrameRoundTripAndCorruptionDetection(t *testing.T) {
	body := []byte("hello world")
	data := appendFrame(nil, 99, entAck, body)
	got, err := readFrame(bufio.NewReader(bytes.NewReader(data)), int64(len(data)), DefaultMaxEntryBytes)
	if err != nil || got.lsn != 99 || got.typ != entAck || !bytes.Equal(got.body, body) || got.size != int64(len(data)) {
		t.Fatalf("got %+v, %v", got, err)
	}

	// Damage to any single byte must be detected, never silently accepted.
	for i := range data {
		mut := append([]byte(nil), data...)
		mut[i] ^= 0x40
		if _, err := readFrame(bufio.NewReader(bytes.NewReader(mut)), int64(len(mut)), DefaultMaxEntryBytes); err == nil {
			t.Fatalf("flipping byte %d went undetected", i)
		}
	}
	// Truncation at any point is reported as torn, or as EOF exactly at the start.
	for cut := 0; cut < len(data); cut++ {
		_, err := readFrame(bufio.NewReader(bytes.NewReader(data[:cut])), int64(cut), DefaultMaxEntryBytes)
		if cut == 0 && errors.Is(err, io.EOF) {
			continue
		}
		if !errors.Is(err, errTorn) {
			t.Fatalf("cut at %d: err = %v, want errTorn", cut, err)
		}
	}
}

func TestFrameRejectsImplausibleLengthWithoutAllocating(t *testing.T) {
	data := appendFrame(nil, 1, entAck, []byte("x"))
	data[0], data[1], data[2], data[3] = 0xff, 0xff, 0xff, 0x7f // ~2 GiB claimed
	if _, err := readFrame(bufio.NewReader(bytes.NewReader(data)), int64(len(data)), DefaultMaxEntryBytes); !errors.Is(err, errBadFrame) {
		t.Fatalf("err = %v, want errBadFrame", err)
	}
	// A length that is legal but larger than the file is a torn write.
	data = appendFrame(nil, 1, entAck, make([]byte, 100))
	if _, err := readFrame(bufio.NewReader(bytes.NewReader(data)), 50, DefaultMaxEntryBytes); !errors.Is(err, errTorn) {
		t.Fatalf("err = %v, want errTorn", err)
	}
}

func TestHeaderRoundTripAndValidation(t *testing.T) {
	h := encodeHeader(walMagic, 12345)
	if lsn, err := decodeHeader(h, walMagic); err != nil || lsn != 12345 {
		t.Fatalf("lsn=%d err=%v", lsn, err)
	}
	if _, err := decodeHeader(h, snapMagic); !errors.Is(err, errBadFrame) {
		t.Fatalf("wrong magic: %v", err)
	}
	if _, err := decodeHeader(h[:10], walMagic); !errors.Is(err, errTorn) {
		t.Fatalf("short: %v", err)
	}
	for i := 0; i < 28; i++ {
		mut := append([]byte(nil), h...)
		mut[i] ^= 1
		if _, err := decodeHeader(mut, walMagic); err == nil {
			t.Fatalf("header byte %d flip undetected", i)
		}
	}
}

func TestDecoderRejectsMalformedBodies(t *testing.T) {
	good := func() []byte {
		var e encoder
		r := sampleRecord()
		encodeRecord(&e, &r)
		return e.b
	}()
	// Every truncation of a valid body must fail cleanly.
	for cut := 0; cut < len(good); cut++ {
		d := decoder{b: good[:cut]}
		decodeRecord(&d)
		if d.done() == nil {
			t.Fatalf("truncation at %d decoded without error", cut)
		}
	}
	// Trailing garbage is an error too.
	d := decoder{b: append(append([]byte(nil), good...), 0)}
	decodeRecord(&d)
	if d.done() == nil {
		t.Fatal("trailing bytes accepted")
	}
	// A huge claimed header count must not allocate or hang.
	var e encoder
	e.str("id")
	e.uvarint(1)
	e.u8(0)
	e.str("")
	e.str("")
	e.varint(0)
	e.varint(0)
	e.nullable(nil)
	e.nullable(nil)
	e.uvarint(1 << 60)
	d = decoder{b: e.b}
	decodeRecord(&d)
	if d.done() == nil {
		t.Fatal("absurd header count accepted")
	}
}

func TestTimeEncodingEdgeCases(t *testing.T) {
	for _, in := range []time.Time{{}, time.Unix(0, 0), time.Unix(1, 1), time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)} {
		var e encoder
		e.time(in)
		d := decoder{b: e.b}
		out := d.time()
		if in.IsZero() != out.IsZero() {
			t.Errorf("zero-ness changed for %v", in)
		}
		if !in.IsZero() && out.Sub(in).Abs() > time.Nanosecond {
			t.Errorf("%v decoded as %v", in, out)
		}
	}
}

func FuzzDecodeRecord(f *testing.F) {
	var e encoder
	r := sampleRecord()
	encodeRecord(&e, &r)
	f.Add(e.b)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01})
	f.Fuzz(func(t *testing.T, data []byte) {
		d := decoder{b: data}
		rec := decodeRecord(&d)
		if d.done() != nil {
			return
		}
		// Anything that decodes must survive a re-encode unchanged.
		var e2 encoder
		encodeRecord(&e2, &rec)
		d2 := decoder{b: e2.b}
		rec2 := decodeRecord(&d2)
		if d2.done() != nil || !recordsEqual(rec, rec2) {
			t.Fatalf("re-encode changed the record: %+v vs %+v", rec, rec2)
		}
	})
}

func FuzzReadFrame(f *testing.F) {
	f.Add(appendFrame(nil, 1, entAck, []byte("x")))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 40))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := bufio.NewReader(bytes.NewReader(data))
		remaining := int64(len(data))
		for {
			fr, err := readFrame(r, remaining, 1<<20)
			if err != nil {
				return
			}
			remaining -= fr.size
			if remaining < 0 {
				t.Fatal("frame larger than the input")
			}
		}
	})
}
