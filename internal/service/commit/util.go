package commit

import (
	"fmt"
	"path"
	"strconv"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

func newTreeEntry(commitOid, rootOid, rootPath string, filename, oidBytes, modeBytes []byte) (*pb.TreeEntry, error) {
	var objectType pb.TreeEntry_EntryType

	mode, err := strconv.ParseInt(string(modeBytes), 8, 32)
	if err != nil {
		return nil, fmt.Errorf("parse mode: %v", err)
	}

	oid := fmt.Sprintf("%02x", oidBytes)

	// Based on https://github.com/git/git/blob/v2.13.1/builtin/ls-tree.c#L67-L87
	switch mode & 0xf000 {
	case 0160000:
		objectType = pb.TreeEntry_COMMIT
	case 040000:
		objectType = pb.TreeEntry_TREE
	default:
		objectType = pb.TreeEntry_BLOB
	}

	return &pb.TreeEntry{
		CommitOid: commitOid,
		RootOid:   rootOid,
		Oid:       oid,
		Path:      []byte(path.Join(rootPath, string(filename))),
		Type:      objectType,
		Mode:      int32(mode),
	}, nil
}
