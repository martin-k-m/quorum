// Package storage is quorum's durable log: the reason a committed write
// survives a crash (design doc §5, §7, milestone M3). It persists exactly
// what Raft safety depends on — the current term, the vote cast in it, and
// the replicated log — to a single append-only file, fsynced before any
// caller may treat a write as durable.
//
// The record format and recovery discipline are deliberately the same shape
// as strata's write-ahead log (github.com/martin-k-m/strata,
// internal/wal.WriteAheadLog): each record is length-prefixed and CRC-checked,
// and a crash can only ever tear the *last* record because the process dies
// mid-write. Recovery reads whole, checksum-valid records until it hits one
// that is short or fails its CRC, then truncates the file to the last good
// record — turning "refuses to open after a hard kill" into "loses at most
// the one write that was in flight."
//
// This package does not know about the Raft protocol beyond the shape of an
// entry; it has no opinion about elections or commitment. It is deliberately
// dumb: append records, replay records. That keeps it simple enough to trust,
// which is the entire point of a durability layer.
package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/martin-k-m/quorum/internal/raft"
)

type recordType byte

const (
	recState    recordType = 1 // term + vote
	recEntry    recordType = 2 // one log entry, appended
	recTruncate recordType = 3 // drop every entry with index >= FromIndex
)

// Storage is a durable log backed by a single file. It is not safe for
// concurrent use; callers (the future server package) are expected to
// serialize access, same as strata's WAL.
type Storage struct {
	f *os.File
}

// Open opens (creating if absent) the durable log at path. Call Recover
// before making any Append call to position the file correctly and to learn
// what was already durable.
func Open(path string) (*Storage, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", path, err)
	}
	return &Storage{f: f}, nil
}

// Recover replays every whole, checksum-valid record in file order, drops a
// torn or corrupt trailing record left by a crash mid-write, and positions
// the file to append after the last good record. It returns the most
// recently persisted term and vote (the zero value of each if none was ever
// saved) and the log as of the last good record — a truncate record removes
// entries from the in-progress result exactly as it did live, so the return
// value reflects the log's final state, not its full history.
func (s *Storage) Recover() (term, vote uint64, entries []raft.Entry, err error) {
	info, err := s.f.Stat()
	if err != nil {
		return 0, 0, nil, fmt.Errorf("storage: stat: %w", err)
	}
	size := info.Size()

	var pos int64
	for pos+8 <= size {
		header := make([]byte, 8)
		if _, err := s.f.ReadAt(header, pos); err != nil {
			break
		}
		payloadLen := int64(binary.BigEndian.Uint32(header[0:4]))
		wantCRC := binary.BigEndian.Uint32(header[4:8])
		if payloadLen <= 0 || pos+8+payloadLen > size {
			break // torn tail: the frame claims more than the file has
		}

		payload := make([]byte, payloadLen)
		if _, err := s.f.ReadAt(payload, pos+8); err != nil {
			break
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			break // corrupt tail
		}

		rec, decErr := decode(payload)
		if decErr != nil {
			// A whole, checksum-valid record that still fails to decode is a
			// bug (a version skew or a logic error), not a crash artifact —
			// unlike a torn or corrupt tail, it is not safe to silently drop.
			return 0, 0, nil, fmt.Errorf("storage: corrupt record at offset %d: %w", pos, decErr)
		}
		switch r := rec.(type) {
		case stateRecord:
			term, vote = r.term, r.vote
		case raft.Entry:
			entries = append(entries, r)
		case truncateRecord:
			cut := 0
			for cut < len(entries) && entries[cut].Index < r.fromIndex {
				cut++
			}
			entries = entries[:cut]
		}
		pos += 8 + payloadLen
	}

	if pos < size {
		if err := s.f.Truncate(pos); err != nil {
			return 0, 0, nil, fmt.Errorf("storage: truncate torn tail: %w", err)
		}
	}
	if _, err := s.f.Seek(pos, 0); err != nil {
		return 0, 0, nil, fmt.Errorf("storage: seek past recovered log: %w", err)
	}
	return term, vote, entries, nil
}

// SaveState durably persists the current term and the vote cast in it. Must
// be called, and fsynced, before a vote is granted or a candidacy begun — the
// two moments Raft safety depends on this being on disk first.
func (s *Storage) SaveState(term, vote uint64) error {
	return s.appendAndSync(encodeState(term, vote))
}

// AppendEntries durably persists new log entries, in order, in a single
// fsynced write. Must happen before a leader counts them toward a quorum or a
// follower acks them.
func (s *Storage) AppendEntries(entries ...raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	frames := make([]byte, 0, 64*len(entries))
	for _, e := range entries {
		frames = append(frames, encodeEntry(e)...)
	}
	return s.appendAndSync(frames)
}

// TruncateFrom durably records that every entry with index >= fromIndex is
// void — the follower side of an AppendEntries conflict, where the leader's
// log has diverged from what was previously persisted here.
func (s *Storage) TruncateFrom(fromIndex uint64) error {
	return s.appendAndSync(encodeTruncate(fromIndex))
}

func (s *Storage) appendAndSync(b []byte) error {
	if _, err := s.f.Write(b); err != nil {
		return fmt.Errorf("storage: write: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("storage: fsync: %w", err)
	}
	return nil
}

// Close syncs and closes the underlying file.
func (s *Storage) Close() error {
	_ = s.f.Sync()
	return s.f.Close()
}

// --- record encoding ---------------------------------------------------------

type stateRecord struct{ term, vote uint64 }
type truncateRecord struct{ fromIndex uint64 }

func frame(t recordType, payload []byte) []byte {
	body := make([]byte, 1+len(payload))
	body[0] = byte(t)
	copy(body[1:], payload)

	out := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(body))
	copy(out[8:], body)
	return out
}

func encodeState(term, vote uint64) []byte {
	p := make([]byte, 16)
	binary.BigEndian.PutUint64(p[0:8], term)
	binary.BigEndian.PutUint64(p[8:16], vote)
	return frame(recState, p)
}

func encodeEntry(e raft.Entry) []byte {
	p := make([]byte, 8+8+4+len(e.Data))
	binary.BigEndian.PutUint64(p[0:8], e.Term)
	binary.BigEndian.PutUint64(p[8:16], e.Index)
	binary.BigEndian.PutUint32(p[16:20], uint32(len(e.Data)))
	copy(p[20:], e.Data)
	return frame(recEntry, p)
}

func encodeTruncate(fromIndex uint64) []byte {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, fromIndex)
	return frame(recTruncate, p)
}

// decode parses one payload (the frame's body, minus length/CRC) into the
// record it represents.
func decode(payload []byte) (any, error) {
	if len(payload) < 1 {
		return nil, fmt.Errorf("empty record")
	}
	switch recordType(payload[0]) {
	case recState:
		p := payload[1:]
		if len(p) != 16 {
			return nil, fmt.Errorf("state record: want 16 bytes, got %d", len(p))
		}
		return stateRecord{
			term: binary.BigEndian.Uint64(p[0:8]),
			vote: binary.BigEndian.Uint64(p[8:16]),
		}, nil
	case recEntry:
		p := payload[1:]
		if len(p) < 20 {
			return nil, fmt.Errorf("entry record: want at least 20 bytes, got %d", len(p))
		}
		dataLen := binary.BigEndian.Uint32(p[16:20])
		if uint32(len(p)-20) != dataLen {
			return nil, fmt.Errorf("entry record: data length mismatch: header says %d, have %d", dataLen, len(p)-20)
		}
		data := append([]byte(nil), p[20:]...)
		return raft.Entry{
			Term:  binary.BigEndian.Uint64(p[0:8]),
			Index: binary.BigEndian.Uint64(p[8:16]),
			Data:  data,
		}, nil
	case recTruncate:
		p := payload[1:]
		if len(p) != 8 {
			return nil, fmt.Errorf("truncate record: want 8 bytes, got %d", len(p))
		}
		return truncateRecord{fromIndex: binary.BigEndian.Uint64(p)}, nil
	default:
		return nil, fmt.Errorf("unknown record type %d", payload[0])
	}
}
