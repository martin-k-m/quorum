package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/martin-k-m/quorum/internal/raft"
)

// The snapshot lives beside the log, not inside it. A snapshot is written whole
// and replaced whole, which is a different discipline from an append-only file
// that is only ever extended, and putting the two in one file would mean the log
// could never be shortened without rewriting the snapshot with it.
func snapPath(logPath string) string { return logPath + ".snap" }
func tempPath(logPath string) string { return logPath + ".snap.tmp" }

// SaveSnapshot writes a snapshot durably, replacing any previous one. The write
// goes to a temporary file that is fsynced and then renamed over the old one,
// so a crash leaves either the whole previous snapshot or the whole new one and
// never a half-written file. The containing directory is fsynced too, because
// on Linux a rename is not durable until the directory entry is.
//
// This must complete before the log is compacted. The snapshot is what makes
// discarding the log prefix safe, so the order is not an optimization: reversed,
// a crash between the two loses every entry the snapshot was meant to replace.
func (s *Storage) SaveSnapshot(snap *raft.Snapshot) error {
	tmp := tempPath(s.path)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("storage: create snapshot temp: %w", err)
	}
	if _, err := f.Write(encodeSnapshot(snap)); err != nil {
		f.Close()
		return fmt.Errorf("storage: write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("storage: fsync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("storage: close snapshot: %w", err)
	}
	if err := os.Rename(tmp, snapPath(s.path)); err != nil {
		return fmt.Errorf("storage: rename snapshot into place: %w", err)
	}
	return syncDir(filepath.Dir(s.path))
}

// LoadSnapshot reads the snapshot beside the log. It returns (nil, nil) when
// there is none, and an error when one exists but fails its checksum: unlike a
// torn tail on the log, a corrupt snapshot cannot be recovered from by dropping
// it, because the log prefix it replaced is already gone.
func (s *Storage) LoadSnapshot() (*raft.Snapshot, error) {
	b, err := os.ReadFile(snapPath(s.path))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: read snapshot: %w", err)
	}
	return decodeSnapshot(b)
}

// Compact rewrites the log with only the entries above fromIndex, plus the
// current term and vote, and swaps it into place atomically. This is what
// actually bounds the disk: recording the compaction in the append-only log
// would leave every byte it was meant to reclaim exactly where it was.
//
// The caller must have written the snapshot first.
func (s *Storage) Compact(fromIndex, term, vote uint64, keep []raft.Entry) error {
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("storage: create log temp: %w", err)
	}
	buf := encodeState(term, vote)
	for _, e := range keep {
		if e.Index <= fromIndex {
			continue
		}
		buf = append(buf, encodeEntry(e)...)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("storage: write compacted log: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("storage: fsync compacted log: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("storage: close compacted log: %w", err)
	}

	// The live handle has to go before the rename. Windows refuses to replace a
	// file that is open, and on Linux holding it would leave the Storage writing
	// into a file with no name. Reopening the original on a failed rename is
	// what keeps a failure here recoverable rather than terminal.
	if err := s.f.Close(); err != nil {
		return fmt.Errorf("storage: close log before swap: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		reopened, rerr := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o600)
		if rerr != nil {
			return fmt.Errorf("storage: rename compacted log into place: %w (and the original could not be reopened: %v)", err, rerr)
		}
		reopened.Seek(0, 2)
		s.f = reopened
		return fmt.Errorf("storage: rename compacted log into place: %w", err)
	}
	swapped, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("storage: reopen compacted log: %w", err)
	}
	if _, err := swapped.Seek(0, 2); err != nil {
		swapped.Close()
		return fmt.Errorf("storage: seek to end of compacted log: %w", err)
	}
	s.f = swapped
	return syncDir(filepath.Dir(s.path))
}

// syncDir fsyncs a directory so a rename inside it is durable. Opening a
// directory for reading is not portable to Windows, where a rename over an
// existing name is already ordered, so a failure to open is not an error here.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return nil
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return nil
	}
	return nil
}

// --- snapshot encoding -------------------------------------------------------

// The layout mirrors the log's records: a length and a CRC32 over everything
// that follows, so a snapshot torn by a crash mid-write is detected rather than
// silently restored as a shorter map.
func encodeSnapshot(s *raft.Snapshot) []byte {
	cfg := raft.EncodeConfiguration(s.Config)
	body := make([]byte, 8+8+4+len(cfg)+4+len(s.Data))
	binary.BigEndian.PutUint64(body[0:8], s.Index)
	binary.BigEndian.PutUint64(body[8:16], s.Term)
	binary.BigEndian.PutUint32(body[16:20], uint32(len(cfg)))
	off := 20
	copy(body[off:], cfg)
	off += len(cfg)
	binary.BigEndian.PutUint32(body[off:off+4], uint32(len(s.Data)))
	off += 4
	copy(body[off:], s.Data)

	out := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(body))
	copy(out[8:], body)
	return out
}

func decodeSnapshot(b []byte) (*raft.Snapshot, error) {
	if len(b) < 8 {
		return nil, fmt.Errorf("storage: snapshot shorter than its header")
	}
	bodyLen := int(binary.BigEndian.Uint32(b[0:4]))
	wantCRC := binary.BigEndian.Uint32(b[4:8])
	if bodyLen <= 0 || 8+bodyLen > len(b) {
		return nil, fmt.Errorf("storage: snapshot truncated: header claims %d bytes, file has %d", bodyLen, len(b)-8)
	}
	body := b[8 : 8+bodyLen]
	if crc32.ChecksumIEEE(body) != wantCRC {
		return nil, fmt.Errorf("storage: snapshot failed its checksum")
	}
	if len(body) < 20 {
		return nil, fmt.Errorf("storage: snapshot body shorter than its fields")
	}
	s := &raft.Snapshot{
		Index: binary.BigEndian.Uint64(body[0:8]),
		Term:  binary.BigEndian.Uint64(body[8:16]),
	}
	cfgLen := int(binary.BigEndian.Uint32(body[16:20]))
	off := 20
	if off+cfgLen+4 > len(body) {
		return nil, fmt.Errorf("storage: snapshot truncated in its configuration")
	}
	cfg, err := raft.DecodeConfiguration(body[off : off+cfgLen])
	if err != nil {
		return nil, fmt.Errorf("storage: snapshot configuration: %w", err)
	}
	s.Config = cfg
	off += cfgLen
	dataLen := int(binary.BigEndian.Uint32(body[off : off+4]))
	off += 4
	if off+dataLen != len(body) {
		return nil, fmt.Errorf("storage: snapshot data length mismatch: header says %d, have %d", dataLen, len(body)-off)
	}
	s.Data = append([]byte(nil), body[off:off+dataLen]...)
	return s, nil
}
