package raftmgr

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
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
		name           string
		setupFunc      func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, map[string]string)
		expectedOutput map[string]string
	}{
		{
			name: "No messages",
			setupFunc: func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, map[string]string) {
				return nil, []raftpb.Message{}, nil
			},
		},
		{
			name: "Empty Entries",
			setupFunc: func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, map[string]string) {
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
			setupFunc: func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, map[string]string) {
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
			setupFunc: func(tempDir string) ([]*gitalypb.LogEntry, []raftpb.Message, map[string]string) {
				// Simulate a log entry dir with files
				fileContents := map[string]string{
					"1": "file1 content",
					"2": "file2 content",
					"3": "file3 content",
				}
				for name, content := range fileContents {
					require.NoError(t, os.WriteFile(filepath.Join(tempDir, name), []byte(content), 0o644))
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
			// Create a temporary directory
			tempDir := testhelper.TempDir(t)
			defer func() { require.NoError(t, os.RemoveAll(tempDir)) }()

			// Execute setup function to prepare messages and any necessary file contents
			entries, messages, expectedContents := tc.setupFunc(tempDir)

			// Setup logger and transport
			logger := testhelper.SharedLogger(t)
			transport := NewNoopTransport(logger, true)

			// Execute the Send operation
			require.NoError(t, transport.Send(context.Background(), func(storage.LSN) string { return tempDir }, messages))

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
						require.True(t, ok)
						// Verify tarball content matches expectations
						verifyPackedData(t, packedData.Packed.GetData(), expectedContents)
					}
				}
			}
		})
	}
}

// verifyPackedData checks if the tarballData contains files with content as expected
func verifyPackedData(t *testing.T, tarballData []byte, expectedContents map[string]string) {
	// Create a tar reader
	buf := bytes.NewReader(tarballData)
	tarReader := tar.NewReader(buf)

	extractedContents := make(map[string]string)

	for {
		// Read the next header
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			// End of tar archive
			break
		}
		require.NoError(t, err)

		// We skip directories since they don't contain data
		if header.Typeflag == tar.TypeDir {
			continue
		}

		// Read file content
		var extractedData bytes.Buffer
		_, err = io.Copy(&extractedData, tarReader)
		require.NoError(t, err)

		// Store extracted content in the map
		extractedContents[header.Name] = extractedData.String()
	}

	require.Equal(t, expectedContents, extractedContents)
}
