package dlq

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Signed logs.
//
// With WALOptions.Signer set, every frame body is followed by a signature and its
// length, so tampering with an entry (or moving one to another position) is
// detected even when the attacker recomputes the CRC, which anyone can. What is
// signed: a domain string, whether the frame is in a segment or a snapshot, the
// entry's LSN (for a snapshot frame, the snapshot's LSN), its type and its body.
// Contiguous LSNs (already required) expose a deleted entry; a snapshot's footer
// carries the record count. What signing cannot show is that the newest entries
// were cut off the end of the log: keep backups if that matters.
//
// Files say whether they are signed in their header flags, so a store can be moved
// to signing without rewriting its log (see WALOptions.AcceptUnsignedLegacy).

const (
	flagSigned  uint32 = 1
	knownFlags         = flagSigned
	sigDomain          = "goguard/wal/v1\x00"
	maxSigBytes        = 1 << 10
)

var (
	// ErrTampered means an entry's signature does not verify: it was changed after
	// it was written, or the store was opened with different keys. It also matches
	// ErrCorrupt, and the store refuses to open; nothing is repaired or discarded.
	ErrTampered = errors.New("dlq: WAL entry failed signature verification")
	// ErrSignerRequired means the log is signed and the store was opened without a
	// Signer that can verify it.
	ErrSignerRequired = errors.New("dlq: the WAL is signed: WALOptions.Signer is required to open it")
	// ErrUnsignedLog means a Signer is set but the log holds unsigned files, and
	// AcceptUnsignedLegacy is off.
	ErrUnsignedLog = errors.New("dlq: the WAL holds unsigned files (set AcceptUnsignedLegacy to migrate them)")
)

func sigInput(snap bool, lsn uint64, typ byte, body []byte) []byte {
	in := make([]byte, 0, len(sigDomain)+10+len(body))
	in = append(in, sigDomain...)
	if snap {
		in = append(in, 's')
	} else {
		in = append(in, 'w')
	}
	in = binary.LittleEndian.AppendUint64(in, lsn)
	in = append(in, typ)
	return append(in, body...)
}

// sealBody returns what goes on disk for a frame body: the body itself when the
// store does not sign, else body | signature | uint16 signature length.
func (w *WALStore) sealBody(snap bool, lsn uint64, typ byte, body []byte) []byte {
	sg := w.opts.Signer
	if sg == nil {
		return body
	}
	sig := sg.Sign(sigInput(snap, lsn, typ, body))
	out := make([]byte, 0, len(body)+len(sig)+2)
	out = append(out, body...)
	out = append(out, sig...)
	return binary.LittleEndian.AppendUint16(out, uint16(len(sig)))
}

// openBody verifies a frame read from a file and returns its body. signed is the
// file's own flag; an unsigned file is only readable when the store allows legacy
// files, which the caller has already checked.
func (w *WALStore) openBody(signed, snap bool, lsn uint64, typ byte, onDisk []byte) ([]byte, error) {
	if !signed {
		return onDisk, nil
	}
	sg := w.opts.Signer
	if sg == nil {
		return nil, ErrSignerRequired
	}
	if len(onDisk) < 2 {
		return nil, ErrTampered
	}
	n := int(binary.LittleEndian.Uint16(onDisk[len(onDisk)-2:]))
	if n == 0 || n > maxSigBytes || n+2 > len(onDisk) {
		return nil, ErrTampered
	}
	body := onDisk[:len(onDisk)-2-n]
	sig := onDisk[len(onDisk)-2-n : len(onDisk)-2]
	if err := sg.Verify(sigInput(snap, lsn, typ, body), sig); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrTampered, err)
	}
	return body, nil
}

// fileFlags is what a file created now gets in its header.
func (w *WALStore) fileFlags() uint32 {
	if w.opts.Signer != nil {
		return flagSigned
	}
	return 0
}

// checkFileFlags decides whether a file with these header flags may be read.
func (w *WALStore) checkFileFlags(flags uint32) (signed bool, err error) {
	signed = flags&flagSigned != 0
	switch {
	case signed && w.opts.Signer == nil:
		return false, ErrSignerRequired
	case !signed && w.opts.Signer != nil:
		if !w.opts.AcceptUnsignedLegacy {
			return false, ErrUnsignedLog
		}
		w.recovery.UnsignedFiles++
	}
	return signed, nil
}
