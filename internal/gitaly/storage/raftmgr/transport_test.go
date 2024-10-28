package raftmgr

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v16/internal/archive"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v16/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestNoopTransport_Send(t *testing.T) {
	t.Parallel()

	mustMarshalProto := func(msg proto.Message) []byte {
		data, err := proto.Marshal(msg)
		if err != nil {
			panic(fmt.Sprintf("failed to marshal proto: %v", err))
		}
		return data
	}

	tests := []struct {
		name      string
		setupFunc func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, testhelper.DirectoryState)
	}{
		{
			name: "No messages",
			setupFunc: func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, testhelper.DirectoryState) {
				return nil, []raftpb.Message{}, nil
			},
		},
		{
			name: "Empty Entries",
			setupFunc: func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, testhelper.DirectoryState) {
				return nil, []raftpb.Message{
					{
						Type:    raftpb.MsgApp,
						From:    2,
						To:      1,
						Term:    1,
						Entries: []raftpb.Entry{}, // Empty Entries
					},
				}, nil
			},
		},
		{
			name: "Messages with already packed data",
			setupFunc: func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, testhelper.DirectoryState) {
				logEntry := &gitalypb.LogEntry{
					RelativePath: "relative-path",
					Operations: []*gitalypb.LogEntry_Operation{
						{
							Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
								CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
									SourcePath:      []byte("source"),
									DestinationPath: []byte("destination"),
								},
							},
						},
					},
				}
				initialMessage := gitalypb.RaftMessageV1{
					Id:            1,
					ClusterId:     "44c58f50-0a8b-4849-bf8b-d5a56198ea7c",
					AuthorityName: "sample-storage",
					PartitionId:   1,
					LogEntry:      logEntry,
					LogData:       &gitalypb.RaftMessageV1_Packed{Packed: &gitalypb.RaftMessageV1_PackedLogData{Data: []byte("already packed data")}},
				}
				messages := []raftpb.Message{
					{
						Type:    raftpb.MsgApp,
						From:    2,
						To:      1,
						Term:    1,
						Index:   1,
						Entries: []raftpb.Entry{{Index: uint64(1), Type: raftpb.EntryNormal, Data: mustMarshalProto(&initialMessage)}},
					},
				}
				return []*gitalypb.LogEntry{logEntry}, messages, nil
			},
		},
		{
			name: "Messages with referenced data",
			setupFunc: func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, testhelper.DirectoryState) {
				// Simulate a log entry dir with files
				fileContents := testhelper.DirectoryState{
					".": {Mode: archive.TarFileMode | archive.ExecuteMode | fs.ModeDir},
					"1": {Mode: archive.TarFileMode, Content: []byte("file1 content")},
					"2": {Mode: archive.TarFileMode, Content: []byte("file2 content")},
					"3": {Mode: archive.TarFileMode, Content: []byte("file3 content")},
				}
				for name, file := range fileContents {
					if file.Content != nil {
						content := file.Content.([]byte)
						require.NoError(t, os.WriteFile(filepath.Join(tempDir, name), content, 0o644))
					}
				}

				// Construct message with ReferencedLogData
				logEntry := &gitalypb.LogEntry{
					RelativePath: "relative-path",
					Operations: []*gitalypb.LogEntry_Operation{
						{
							Operation: &gitalypb.LogEntry_Operation_CreateHardLink_{
								CreateHardLink: &gitalypb.LogEntry_Operation_CreateHardLink{
									SourcePath:      []byte("source"),
									DestinationPath: []byte("destination"),
								},
							},
						},
					},
				}
				initialMessage := gitalypb.RaftMessageV1{
					Id:            1,
					ClusterId:     "44c58f50-0a8b-4849-bf8b-d5a56198ea7c",
					AuthorityName: "sample-storage",
					PartitionId:   1,
					LogEntry:      logEntry,
					LogData:       &gitalypb.RaftMessageV1_Referenced{Referenced: &gitalypb.RaftMessageV1_ReferencedLogData{}},
				}

				messages := []raftpb.Message{
					{
						Type:  raftpb.MsgApp,
						From:  2,
						To:    1,
						Term:  1,
						Index: 1,
						Entries: []raftpb.Entry{
							{Index: uint64(1), Type: raftpb.EntryNormal, Data: mustMarshalProto(&initialMessage)},
						},
					},
				}
				return []*gitalypb.LogEntry{logEntry}, messages, fileContents
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Create a temporary directory
			tempDir := testhelper.TempDir(t)

			// Execute setup function to prepare messages and any necessary file contents
			entries, messages, expectedContents := tc.setupFunc(tempDir)

			// Setup logger and transport
			logger := testhelper.SharedLogger(t)
			transport := NewNoopTransport(logger, true)

			// Execute the Send operation
			require.NoError(t, transport.Send(testhelper.Context(t), func(storage.LSN) string { return tempDir }, messages))

			// Fetch recorded messages for verification
			recordedMessages := transport.GetRecordedMessages()

			require.Len(t, recordedMessages, len(messages))

			// Messages must be sent in order.
			for i := range messages {
				require.Equal(t, messages[i].Type, recordedMessages[i].Type)
				require.Equal(t, messages[i].From, recordedMessages[i].From)
				require.Equal(t, messages[i].To, recordedMessages[i].To)
				require.Equal(t, messages[i].Term, recordedMessages[i].Term)
				require.Equal(t, messages[i].Index, recordedMessages[i].Index)

				if len(messages[i].Entries) == 0 {
					require.Empty(t, recordedMessages[i].Entries)
				} else {
					var resultMessage gitalypb.RaftMessageV1
					require.NoError(t, proto.Unmarshal(recordedMessages[i].Entries[0].Data, &resultMessage))

					testhelper.ProtoEqual(t, entries[i], resultMessage.GetLogEntry())

					packedData, ok := resultMessage.GetLogData().(*gitalypb.RaftMessageV1_Packed)
					require.True(t, ok)

					tarballData := packedData.Packed.GetData()
					require.NotEmpty(t, tarballData)

					// Optionally verify packed data if expected
					if expectedContents != nil {
						var resultMessage gitalypb.RaftMessageV1
						require.NoError(t, proto.Unmarshal(recordedMessages[0].Entries[0].Data, &resultMessage))

						packedData, ok := resultMessage.GetLogData().(*gitalypb.RaftMessageV1_Packed)
						require.True(t, ok, "packed data must have packed type")

						// Verify tarball content matches expectations
						reader := bytes.NewReader(packedData.Packed.GetData())
						testhelper.RequireTarState(t, reader, expectedContents)
					}
				}
			}
		})
	}
}
