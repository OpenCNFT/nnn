package packfile

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/command"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitcmd"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
)

// Index is an in-memory representation of a packfile .idx file.
type Index struct {
	// ID is the packfile ID. For pack-123abc.idx, this would be 123abc.
	ID           string
	packBase     string
	checksumSize int
	// Objects holds the list of objects in the packfile in index order, i.e. sorted by OID
	Objects []*Object
	// Objects holds the list of objects in the packfile in packfile order, i.e. sorted by packfile offset
	PackfileOrder []*Object
	*IndexBitmap
}

// ReadIndex is a wrapper for ReadIndexWithGitCmdFactory with a nil CommandFactory.
// It uses Git binary in the system PATH.
func ReadIndex(logger log.Logger, idxPath string) (*Index, error) {
	idx, err := ReadIndexWithGitCmdFactory(nil, nil, logger, idxPath)
	return idx, err
}

// ReadIndexWithGitCmdFactory opens a packfile .idx file and loads its contents into
// memory. In doing so it will also open and read small amounts of data
// from the .pack file itself.
func ReadIndexWithGitCmdFactory(cmdFactory gitcmd.CommandFactory, repo *localrepo.Repo, logger log.Logger, idxPath string) (*Index, error) {
	// When using the Git command factory for git-show-index, a repository must be provided .
	// Failure to do so can result in a segmentation fault.
	if cmdFactory != nil && repo == nil {
		return nil, fmt.Errorf("must use Git command factory without repo")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	objectHash, err := repo.ObjectHash(ctx)
	if err != nil {
		return nil, err
	}

	// checksumSize is checksum size in bytes
	checksumSize := objectHash.EncodedLen() / 2

	idxFileRegex := regexp.MustCompile(`\A` + git.PackFileRegexCore + `\.idx\z`)

	reMatches := idxFileRegex.FindStringSubmatch(idxPath)
	if len(reMatches) == 0 {
		return nil, fmt.Errorf("invalid idx filename: %q", idxPath)
	}

	idx := &Index{
		packBase:     reMatches[1] + reMatches[2],
		ID:           reMatches[2],
		checksumSize: checksumSize,
	}

	f, err := os.Open(idx.packBase + ".idx")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(int64(-2*checksumSize), io.SeekEnd); err != nil {
		return nil, err
	}

	packID, err := readN(f, checksumSize)
	if err != nil {
		return nil, err
	}

	if actual := hex.EncodeToString(packID); idx.ID != actual {
		return nil, fmt.Errorf("expected idx to go with pack %s, got %s", idx.ID, actual)
	}

	count, err := idx.numPackObjects()
	if err != nil {
		return nil, err
	}

	// TODO use a data structure other than a Go slice to hold the index
	// entries? Go slices use int as their index type, and int may not be
	// able to hold MaxUint32.
	if count > math.MaxInt32 {
		return nil, fmt.Errorf("too many objects in to fit in Go slice: %d", count)
	}
	idx.Objects = make([]*Object, count)

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var stderr bytes.Buffer
	var showIndex *command.Command

	if cmdFactory == nil {
		showIndex, err = command.New(
			ctx, logger,
			[]string{"git", "show-index"},
			command.WithStdin(f), command.WithSetupStdout(), command.WithStderr(&stderr),
		)
	} else {
		showIndex, err = cmdFactory.New(ctx, repo, gitcmd.Command{
			Name: "show-index",
		},
			gitcmd.WithStdin(f), gitcmd.WithSetupStdout(), gitcmd.WithStderr(&stderr),
		)
	}

	if err != nil {
		return nil, structerr.New("spawning git-show-index: %w: %s", err, stderr.String())
	}

	i := 0
	if err := ParseIndex(showIndex, func(obj *Object) {
		idx.Objects[i] = obj
		i++
	}); err != nil {
		return nil, err
	}

	if err := showIndex.Wait(); err != nil {
		return nil, structerr.New("waiting git-show-index: %w: %s", err, stderr.String())
	}

	if i != len(idx.Objects) {
		return nil, fmt.Errorf("expected %d objects in output of git show-index, got %d", len(idx.Objects), i)
	}

	return idx, nil
}

// ParseIndex parses the output of `git-show-index(1)` command. The caller needs to provide the output stream from that
// command. This function triggers the input callback on each object it finds. The order of objects follows the output
// of the command.
func ParseIndex(stdout io.Reader, callback func(*Object)) error {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.SplitN(line, " ", 3)
		if len(split) != 3 {
			return fmt.Errorf("unable to parse show-index line: %q", line)
		}

		offset, err := strconv.ParseUint(split[0], 10, 64)
		if err != nil {
			return err
		}
		oid := split[1]

		callback(&Object{OID: oid, Offset: offset})
	}
	return scanner.Err()
}

func (idx *Index) numPackObjects() (uint32, error) {
	f, err := idx.openPack()
	if err != nil {
		return 0, err
	}
	defer f.Close()

	const sizeOffset = 8
	if _, err := f.Seek(sizeOffset, io.SeekStart); err != nil {
		return 0, err
	}

	return readUint32(f)
}

func (idx *Index) openPack() (f *os.File, err error) {
	packPath := idx.packBase + ".pack"
	f, err = os.Open(packPath)
	if err != nil {
		return nil, err
	}

	defer func(f *os.File) {
		if err != nil {
			f.Close()
		}
	}(f) // Bind f early so that we can do "return nil, err".

	const headerLen = 8
	header, err := readN(f, headerLen)
	if err != nil {
		return nil, err
	}

	const sig = "PACK\x00\x00\x00\x02"
	if s := string(header); s != sig {
		return nil, fmt.Errorf("unexpected pack signature %q", s)
	}

	if _, err := f.Seek(int64(-idx.checksumSize), io.SeekEnd); err != nil {
		return nil, err
	}

	sum, err := readN(f, idx.checksumSize)
	if err != nil {
		return nil, err
	}

	if s := hex.EncodeToString(sum); s != idx.ID {
		return nil, fmt.Errorf("unexpected trailing checksum in .pack: %s", s)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return f, nil
}

func readUint32(r io.Reader) (uint32, error) {
	buf, err := readN(r, 4)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(buf), nil
}

func readN(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

// BuildPackfileOrder populates the PackfileOrder field.
func (idx *Index) BuildPackfileOrder() {
	if len(idx.PackfileOrder) > 0 {
		return
	}

	idx.PackfileOrder = make([]*Object, len(idx.Objects))
	copy(idx.PackfileOrder, idx.Objects)
	sort.Sort(offsetOrder(idx.PackfileOrder))
}

type offsetOrder []*Object

func (oo offsetOrder) Len() int           { return len(oo) }
func (oo offsetOrder) Less(i, j int) bool { return oo[i].Offset < oo[j].Offset }
func (oo offsetOrder) Swap(i, j int)      { oo[i], oo[j] = oo[j], oo[i] }

// LabelObjectTypes tries to label each object in the index with its
// object type, using the packfile bitmap. Returns an error if there is
// no packfile .bitmap file.
func (idx *Index) LabelObjectTypes() error {
	if err := idx.LoadBitmap(); err != nil {
		return err
	}

	idx.BuildPackfileOrder()

	for _, t := range []struct {
		objectType ObjectType
		bmp        *Bitmap
	}{
		{TCommit, idx.IndexBitmap.Commits},
		{TTree, idx.IndexBitmap.Trees},
		{TBlob, idx.IndexBitmap.Blobs},
		{TTag, idx.IndexBitmap.Tags},
	} {
		if err := t.bmp.Scan(func(i int) error {
			obj := idx.PackfileOrder[i]
			if obj.Type != TUnknown {
				return fmt.Errorf("type already set for object %v", obj)
			}

			obj.Type = t.objectType

			return nil
		}); err != nil {
			return err
		}
	}

	for _, obj := range idx.PackfileOrder {
		if obj.Type == TUnknown {
			return fmt.Errorf("object missing type label: %v", obj)
		}
	}

	return nil
}
