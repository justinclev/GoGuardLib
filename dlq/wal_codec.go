package dlq

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

// On-disk formats.
//
// Segment file:   header(32) frame*
// Snapshot file:  header(32) frame* (snapRecord)  final frame (snapEnd)
//
// Header: magic(8) version u32 flags u32 lsn u64 crc u32 pad u32, where crc
// covers the first 24 bytes. For a segment lsn is the LSN of its first entry;
// for a snapshot it is the LSN of the last entry the snapshot includes.
//
// Frame: length u32 | crc u32 | lsn u64 | type u8 | body
// length counts lsn+type+body. crc is CRC-32C over length, lsn, type and body,
// so a damaged length is detected as reliably as damaged data.
const (
	walMagic      = "GGUARDWL"
	snapMagic     = "GGUARDSN"
	formatVersion = 1
	headerSize    = 32
	frameOverhead = 8 // length + crc
	frameMeta     = 9 // lsn + type

	// DefaultMaxEntryBytes bounds a single log entry, so a corrupt length can
	// never make recovery allocate an absurd amount of memory.
	DefaultMaxEntryBytes = 64 << 20
)

const (
	entAppend     byte = 1
	entLease      byte = 2
	entCheckpoint byte = 3
	entAck        byte = 4
	entNack       byte = 5
	entRelease    byte = 6
	entPark       byte = 7
	entRequeue    byte = 8
	entDiscard    byte = 9

	snapRecord byte = 64
	snapEnd    byte = 65
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	// errTorn: the data ends in the middle of a frame or header.
	errTorn = errors.New("dlq: truncated frame")
	// errBadFrame: the frame is complete but its checksum or length is wrong.
	errBadFrame = errors.New("dlq: damaged frame")
	// errBadEntry: the frame is intact but its body does not decode.
	errBadEntry = errors.New("dlq: undecodable entry")
)

func encodeHeader(magic string, lsn uint64) []byte { return encodeHeaderFlags(magic, lsn, 0) }

func encodeHeaderFlags(magic string, lsn uint64, flags uint32) []byte {
	h := make([]byte, headerSize)
	copy(h, magic)
	binary.LittleEndian.PutUint32(h[8:], formatVersion)
	binary.LittleEndian.PutUint32(h[12:], flags)
	binary.LittleEndian.PutUint64(h[16:], lsn)
	binary.LittleEndian.PutUint32(h[24:], crc32.Checksum(h[:24], castagnoli))
	return h
}

// decodeHeader validates a header and returns its lsn.
func decodeHeader(h []byte, magic string) (uint64, error) {
	lsn, _, err := decodeHeaderFlags(h, magic)
	return lsn, err
}

// decodeHeaderFlags validates a header and returns its lsn and flags.
func decodeHeaderFlags(h []byte, magic string) (uint64, uint32, error) {
	if len(h) < headerSize {
		return 0, 0, errTorn
	}
	if string(h[:8]) != magic {
		return 0, 0, fmt.Errorf("%w: bad magic", errBadFrame)
	}
	if binary.LittleEndian.Uint32(h[24:]) != crc32.Checksum(h[:24], castagnoli) {
		return 0, 0, fmt.Errorf("%w: header checksum mismatch", errBadFrame)
	}
	if v := binary.LittleEndian.Uint32(h[8:]); v != formatVersion {
		return 0, 0, fmt.Errorf("dlq: unsupported format version %d", v)
	}
	flags := binary.LittleEndian.Uint32(h[12:])
	if flags&^knownFlags != 0 {
		return 0, 0, fmt.Errorf("dlq: unsupported file flags %#x (written by a newer version?)", flags)
	}
	return binary.LittleEndian.Uint64(h[16:]), flags, nil
}

// appendFrame appends one frame to dst.
func appendFrame(dst []byte, lsn uint64, typ byte, body []byte) []byte {
	start := len(dst)
	var hdr [frameOverhead + frameMeta]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(frameMeta+len(body)))
	binary.LittleEndian.PutUint64(hdr[8:], lsn)
	hdr[16] = typ
	dst = append(dst, hdr[:]...)
	dst = append(dst, body...)
	// crc over length + (lsn,type,body)
	c := crc32.Update(0, castagnoli, dst[start:start+4])
	c = crc32.Update(c, castagnoli, dst[start+frameOverhead:])
	binary.LittleEndian.PutUint32(dst[start+4:], c)
	return dst
}

// frame is a decoded frame.
type frame struct {
	lsn  uint64
	typ  byte
	body []byte
	size int64 // bytes the frame occupies on disk
}

// readFrame reads the next frame. remaining is how many bytes the file still
// holds, used to reject an impossible length without allocating for it. It
// returns io.EOF only at a clean frame boundary.
func readFrame(r *bufio.Reader, remaining int64, maxEntry int) (frame, error) {
	var hdr [frameOverhead]byte
	n, err := io.ReadFull(r, hdr[:])
	if err != nil {
		if n == 0 && errors.Is(err, io.EOF) {
			return frame{}, io.EOF
		}
		return frame{}, errTorn
	}
	length := int64(binary.LittleEndian.Uint32(hdr[0:]))
	if length < frameMeta || length > int64(maxEntry)+frameMeta {
		return frame{}, fmt.Errorf("%w: implausible length %d", errBadFrame, length)
	}
	if length > remaining-frameOverhead {
		return frame{}, errTorn
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return frame{}, errTorn
	}
	c := crc32.Update(0, castagnoli, hdr[0:4])
	c = crc32.Update(c, castagnoli, payload)
	if c != binary.LittleEndian.Uint32(hdr[4:]) {
		return frame{}, fmt.Errorf("%w: checksum mismatch", errBadFrame)
	}
	return frame{
		lsn:  binary.LittleEndian.Uint64(payload[0:]),
		typ:  payload[8],
		body: payload[frameMeta:],
		size: frameOverhead + length,
	}, nil
}

// encoder builds entry bodies.
type encoder struct{ b []byte }

func (e *encoder) u8(v byte)        { e.b = append(e.b, v) }
func (e *encoder) uvarint(v uint64) { e.b = binary.AppendUvarint(e.b, v) }
func (e *encoder) varint(v int64)   { e.b = binary.AppendVarint(e.b, v) }
func (e *encoder) str(s string) {
	e.uvarint(uint64(len(s)))
	e.b = append(e.b, s...)
}

// nullable writes nil and empty differently: 0 for nil, len+1 otherwise.
func (e *encoder) nullable(b []byte) {
	if b == nil {
		e.uvarint(0)
		return
	}
	e.uvarint(uint64(len(b)) + 1)
	e.b = append(e.b, b...)
}

// time writes 0 for the zero time, else UnixNano (an exact 0 is nudged to 1).
func (e *encoder) time(t time.Time) {
	if t.IsZero() {
		e.varint(0)
		return
	}
	ns := t.UnixNano()
	if ns == 0 {
		ns = 1
	}
	e.varint(ns)
}

// decoder reads entry bodies, remembering the first error.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) fail() {
	if d.err == nil {
		d.err = errBadEntry
	}
}

func (d *decoder) u8() byte {
	if d.err != nil || len(d.b) < 1 {
		d.fail()
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) varint() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

// take returns the next n bytes, refusing any n larger than what is left.
func (d *decoder) take(n uint64) []byte {
	if d.err != nil || n > uint64(len(d.b)) {
		d.fail()
		return nil
	}
	out := d.b[:n]
	d.b = d.b[n:]
	return out
}

func (d *decoder) str() string { return string(d.take(d.uvarint())) }

func (d *decoder) nullable() []byte {
	n := d.uvarint()
	if n == 0 {
		return nil
	}
	return append([]byte{}, d.take(n-1)...)
}

func (d *decoder) time() time.Time {
	ns := d.varint()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

func (d *decoder) done() error {
	if d.err == nil && len(d.b) != 0 {
		d.fail()
	}
	return d.err
}

func encodeRecord(e *encoder, r *Record) {
	e.str(r.ID)
	e.uvarint(r.Seq)
	e.u8(byte(r.State))
	e.str(r.Source.Kind)
	e.str(r.Source.Name)
	e.varint(int64(r.Source.Partition))
	e.varint(r.Source.Offset)
	e.nullable(r.Key)
	e.nullable(r.Value)
	e.uvarint(uint64(len(r.Headers)))
	for _, h := range r.Headers {
		e.str(h.Key)
		e.nullable(h.Value)
	}
	e.str(r.OrderKey)
	e.str(r.BlockedOn)
	e.uvarint(uint64(r.Attempts))
	e.time(r.FirstFailed)
	e.time(r.LastFailed)
	e.time(r.NextAttempt)
	e.str(r.LastError)
	e.nullable(r.Checkpoint)
	e.uvarint(uint64(r.SchemaVersion))
}

func decodeRecord(d *decoder) Record {
	var r Record
	r.ID = d.str()
	r.Seq = d.uvarint()
	r.State = State(d.u8())
	r.Source.Kind = d.str()
	r.Source.Name = d.str()
	r.Source.Partition = int32(d.varint())
	r.Source.Offset = d.varint()
	r.Key = d.nullable()
	r.Value = d.nullable()
	if n := d.uvarint(); n > 0 {
		if n > uint64(len(d.b)) { // every header needs at least two bytes
			d.fail()
			return r
		}
		r.Headers = make([]Header, 0, n)
		for i := uint64(0); i < n && d.err == nil; i++ {
			r.Headers = append(r.Headers, Header{Key: d.str(), Value: d.nullable()})
		}
	}
	r.OrderKey = d.str()
	r.BlockedOn = d.str()
	r.Attempts = int(d.uvarint())
	r.FirstFailed = d.time()
	r.LastFailed = d.time()
	r.NextAttempt = d.time()
	r.LastError = d.str()
	r.Checkpoint = d.nullable()
	r.SchemaVersion = int(d.uvarint())
	if d.err == nil && (r.State != Pending && r.State != Parked && r.State != Leased || r.Attempts < 0) {
		d.fail()
	}
	return r
}
