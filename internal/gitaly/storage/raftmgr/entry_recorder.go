package raftmgr

import (
	"sync"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// EntryRecorder is a utility for recording and classifying log entries processed by the Raft manager. In addition to
// standard log entries, Raft may generate internal entries such as configuration changes or empty logs for verification
// purposes. These internal entries are backfilled into the Write-Ahead Log (WAL), occupying Log Sequence Number (LSN)
// slots. Consequently, the LSN sequence may diverge from expectations when Raft is enabled.
//
// This recorder is equipped with the capability to offset the LSN, which is particularly useful in testing environments
// to mitigate differences in LSN sequences. It is strongly advised that this feature be restricted to testing purposes
// and not utilized in production or other non-testing scenarios.
type EntryRecorder struct {
	mu    sync.Mutex // Mutex for safe concurrent access
	Items []Item     // Slice to store recorded log entries
}

// Item represents an entry recorded by the LogEntryRecorder, with a flag indicating if it's from Raft.
type Item struct {
	FromRaft bool
	LSN      storage.LSN
	Entry    *gitalypb.LogEntry
}

// NewEntryRecorder returns a new instance of NewEntryRecorder.
func NewEntryRecorder() *EntryRecorder {
	return &EntryRecorder{}
}

// Record logs an entry, marking it as originating from Raft if specified.
// If the LSN is from the past, it removes all entries after that LSN.
func (r *EntryRecorder) Record(fromRaft bool, lsn storage.LSN, entry *gitalypb.LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Trim items that have an LSN greater than the current LSN
	if len(r.Items) > 0 {
		idx := -1
		for i, itm := range r.Items {
			if itm.LSN >= lsn {
				idx = i
				break
			}
		}
		if idx != -1 {
			r.Items = r.Items[:idx]
		}
	}

	// Append the new entry
	r.Items = append(r.Items, Item{
		FromRaft: fromRaft,
		LSN:      lsn,
		Entry:    proto.Clone(entry).(*gitalypb.LogEntry),
	})
}

// Offset adjusts the log sequence number (LSN) by accounting for internal Raft entries that may occupy slots.
func (r *EntryRecorder) Offset(lsn storage.LSN) storage.LSN {
	r.mu.Lock()
	defer r.mu.Unlock()

	offset := lsn
	for _, itm := range r.Items {
		if itm.FromRaft {
			offset++
		}
		if itm.LSN >= offset {
			break
		}
	}
	return offset
}

// Metadata returns metadata of an entry if it exists.
func (r *EntryRecorder) Metadata(lsn storage.LSN) *gitalypb.LogEntry_Metadata {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, itm := range r.Items {
		if itm.LSN == lsn {
			return itm.Entry.GetMetadata()
		}
	}
	return nil
}

// Latest returns the latest recorded LSN.
func (r *EntryRecorder) Latest() storage.LSN {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.Items) == 0 {
		return 0
	}
	return r.Items[len(r.Items)-1].LSN
}

// Len returns the length of recorded entries.
func (r *EntryRecorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.Items)
}

// IsFromRaft returns true if the asserting LSN is an entry emitted by Raft.
func (r *EntryRecorder) IsFromRaft(lsn storage.LSN) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, itm := range r.Items {
		if itm.LSN == lsn {
			return itm.FromRaft
		}
	}
	return false
}

// FromRaft retrieves all log entries that originated from the Raft system.
func (r *EntryRecorder) FromRaft() map[storage.LSN]*gitalypb.LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	raftEntries := map[storage.LSN]*gitalypb.LogEntry{}
	for _, itm := range r.Items {
		if itm.FromRaft {
			raftEntries[itm.LSN] = itm.Entry
		}
	}
	return raftEntries
}
