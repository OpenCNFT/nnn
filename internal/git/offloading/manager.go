package offloading

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v16/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/config"
	"gitlab.com/gitlab-org/labkit/log"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Manager manages the offloading of Git data to external storage
type Manager struct {
	filter      string
	filterLimit int
	filterTo    string

	gcpClient *storage.Client
	bucket    string

	catfileCache catfile.Cache
	ctx          context.Context
	repo         *localrepo.Repo
}

// NewOffloadingManager creates a new OffloadingManager instance
func NewOffloadingManager(ctx context.Context, repo *localrepo.Repo) (*Manager, error) {
	filterLimitStr := os.Getenv("OFFLOADING_POC_FILTER_LIMIT")
	var err error
	var filterLimit int
	var filter string
	if len(filterLimitStr) == 0 {
		return nil, fmt.Errorf("I need a filter limit OFFLOADING_POC_FILTER_LIMIT")
	} else {
		filterLimit, err = strconv.Atoi(filterLimitStr)
		if err != nil {
			return nil, fmt.Errorf("OFFLOADING_POC_FILTER_LIMIT is not a number: %s", filterLimitStr)
		}
		filter = fmt.Sprintf("blob:limit=%d", filterLimit)
	}
	filterTo := os.Getenv("OFFLOADING_POC_FILTER_TO_PATH")
	if len(filterTo) == 0 {
		filterTo = "/tmp/pack"
	}
	bucket := os.Getenv("OFFLOADING_POC_BUCKET")
	if len(bucket) == 0 {
		bucket = "blob_offloads"
	}

	// new Google Storage Client
	// Use Google Application Default Credentials to authorize and authenticate the client.
	// More information about Application Default Credentials and how to enable is at
	// https://developers.google.com/identity/protocols/application-default-credentials.
	gsClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("google storage client error: %w", err)
	}
	cfg := config.Cfg{}
	cfg.Git.CatfileCacheSize = 100
	catfileCache := catfile.NewCache(cfg)

	return &Manager{
		filter:       filter,
		filterLimit:  filterLimit,
		filterTo:     filterTo,
		gcpClient:    gsClient,
		bucket:       bucket,
		catfileCache: catfileCache,

		ctx:  ctx,
		repo: repo,
	}, nil
}

func (m *Manager) FindObjects() []string {

	//TODO use channel and goroutine to optimize ??
	//largeBlobChan := make(chan git.ObjectID)
	res := make([]string, 0)

	revlistOptions := []gitpipe.RevlistOption{
		gitpipe.WithObjects(),
		gitpipe.WithBlobLimit(m.filterLimit),
		gitpipe.WithFilterPrintOmitted(),
	}

	revlistIter := gitpipe.Revlist(m.ctx, m.repo, []string{"--all"}, revlistOptions...)
	for revlistIter.Next() {
		oid := revlistIter.Result().OID.String()
		if strings.HasPrefix(oid, "~") {
			log.Info(oid)
			res = append(res, strings.TrimPrefix(oid, "~"))
		}

	}
	// TODO avoid double uploading
	//defer close(largeBlobChan)

	return res
}

func (m *Manager) CatBlobs(oids []string) (it gitpipe.CatfileObjectIterator, err error) {

	catfileInfo := make([]gitpipe.CatfileInfoResult, 0)
	for _, oid := range oids {
		catfileInfo = append(catfileInfo, gitpipe.CatfileInfoResult{
			ObjectInfo: &catfile.ObjectInfo{Oid: git.ObjectID(oid), Type: "blob"},
		})
	}

	objectReader, _, err := m.catfileCache.ObjectReader(m.ctx, m.repo)
	if err != nil {
		return nil, err
	}

	it, err = gitpipe.CatfileObject(m.ctx, objectReader, gitpipe.NewCatfileInfoIterator(m.ctx, catfileInfo))
	if err != nil {
		return nil, err
	}

	return it, nil
}

// UploadObjects upload the blobs on to cloud storage
func (m *Manager) UploadObjects() error {
	defer m.catfileCache.Evict()

	client, err := storage.NewClient(m.ctx)
	if err != nil {
		return err
	}

	bucketClient := client.Bucket(m.bucket)

	uploadList := m.FindObjects()
	objIt, err := m.CatBlobs(uploadList)
	if err != nil {
		return err
	}

	for objIt.Next() {

		result := objIt.Result()
		objId := result.ObjectID()

		bucketPath := fmt.Sprintf("%s/%s", m.repo.GetRelativePath(), objId)
		wc := bucketClient.Object(bucketPath).NewWriter(m.ctx)

		if _, err := io.Copy(wc, result); err != nil {
			return fmt.Errorf("io.Copy: %v", err)
		}
		log.Info("Uploading object ", objId)
		if err := wc.Close(); err != nil {
			return fmt.Errorf("Writer.Close: %v", err)
		}
	}

	// close gs client
	return nil
}

// BlobFilterOptions returns the flags for repacking without large blobs
func (m *Manager) BlobFilterOptions() []git.Option {
	opts := []git.Option{
		git.Flag{Name: "-a"},
		git.Flag{Name: "-d"},
		git.ValueFlag{Name: "--filter", Value: m.filter},
		git.ValueFlag{Name: "--filter-to", Value: m.filterTo},
	}

	return opts
}

// CreatePromisorFile is the next step after repacking without large blobs
// TODO Ideally, I think this should be done in git repack, say give it a flag and it create the promisor file
// But for now, we add it in gitaly
func (m *Manager) CreatePromisorFile() error {
	// find the name for the promisor file
	var err error
	repoPath, err := m.repo.Path()
	packFolder := filepath.Join(repoPath, "objects", "pack")
	packFiles := make([]string, 0)
	err = filepath.Walk(packFolder, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if filepath.Ext(info.Name()) == ".pack" {
				packFiles = append(packFiles, info.Name())
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// add the promisor file for each packFiles
	for _, f := range packFiles {
		fileName := strings.TrimSuffix(f, ".pack")
		promisorFile := fmt.Sprintf("%s/%s.promisor", packFolder, fileName)
		_, err := os.Create(promisorFile)
		if err != nil {
			return err
		}
	}

	return nil
}
