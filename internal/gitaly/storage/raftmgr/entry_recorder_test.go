package raftmgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

func TestLogEntryRecorder(t *testing.T) {
	testCases := []struct {
		desc           string
		records        []Item
		initialLSN     storage.LSN
		expectedOffset storage.LSN
	}{
		{
			desc:           "no entries",
			records:        []Item{},
			initialLSN:     1,
			expectedOffset: 1,
		},
		{
			desc: "no internal raft entries",
			records: []Item{
				{FromRaft: false, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 2, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     1,
			expectedOffset: 1,
		},
		{
			desc: "no internal raft entries",
			records: []Item{
				{FromRaft: false, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 2, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     2,
			expectedOffset: 2,
		},
		{
			desc: "all raft entries",
			records: []Item{
				{FromRaft: true, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: true, LSN: 2, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     1,
			expectedOffset: 3, // Two raft entries increment the offset by 2
		},
		{
			desc: "mix of raft and non-raft, with raft at beginning",
			records: []Item{
				{FromRaft: true, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 2, Entry: &gitalypb.LogEntry{}},
				{FromRaft: true, LSN: 3, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     1,
			expectedOffset: 2,
		},
		{
			desc: "mix of raft and non-raft, with raft at beginning",
			records: []Item{
				{FromRaft: true, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 2, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 3, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     2,
			expectedOffset: 3,
		},
		{
			desc: "mix of raft and non-raft, with raft at beginning",
			records: []Item{
				{FromRaft: true, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 2, Entry: &gitalypb.LogEntry{}},
				{FromRaft: true, LSN: 3, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     3,
			expectedOffset: 5,
		},
		{
			desc: "mix of raft and non-raft, no raft at beginning",
			records: []Item{
				{FromRaft: false, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: true, LSN: 2, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 3, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     1,
			expectedOffset: 1,
		},
		{
			desc: "mix of raft and non-raft, no raft at beginning",
			records: []Item{
				{FromRaft: false, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: true, LSN: 2, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 3, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     3,
			expectedOffset: 4,
		},
		{
			desc: "initial LSN beyond recorded entries",
			records: []Item{
				{FromRaft: false, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: true, LSN: 2, Entry: &gitalypb.LogEntry{}},
			},
			initialLSN:     3,
			expectedOffset: 4,
		},
		{
			desc: "replace entries with a past LSN",
			records: []Item{
				{FromRaft: true, LSN: 1, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 2, Entry: &gitalypb.LogEntry{}},
				{FromRaft: false, LSN: 3, Entry: &gitalypb.LogEntry{}},
				{FromRaft: true, LSN: 2, Entry: &gitalypb.LogEntry{}}, // Insert with past LSN, should remove LSN 2 and 3 entry
			},
			initialLSN:     1,
			expectedOffset: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			recorder := EntryRecorder{}

			// Record entries as specified in the test case
			for _, record := range tc.records {
				recorder.Record(record.FromRaft, record.LSN, record.Entry)
			}

			// Validate the offset
			offset := recorder.Offset(tc.initialLSN)
			require.Equal(t, tc.expectedOffset, offset)
		})
	}
}
