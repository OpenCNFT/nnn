package fshistory

import (
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

type Transaction struct {
	history       *History
	readLSN       storage.LSN
	root          *node
	modifiedNodes map[string]*node
}

func (tx *Transaction) Commit(commitLSN storage.LSN) {
	if len(tx.modifiedNodes) == 0 {
		// This transaction didn't perform any writes.
		return
	}

	// Record the transaction's LSN on the modified nodes.
	modifiedPaths := make(map[string]struct{}, len(tx.modifiedNodes))
	for path, node := range tx.modifiedNodes {
		node.lsn = commitLSN

		// Remove the path from the index of the previous updating LSN.
		if previousLSN, ok := tx.history.lsnByPath[path]; ok {
			delete(tx.history.pathsByLSN[previousLSN], path)

			if len(tx.history.pathsByLSN[previousLSN]) == 0 {
				// If all of the paths are already update by later LSNs,
				// drop this index entry.
				delete(tx.history.pathsByLSN, previousLSN)
			}
		}

		tx.history.lsnByPath[path] = commitLSN
		modifiedPaths[path] = struct{}{}
	}

	tx.history.root = tx.root
	tx.history.pathsByLSN[commitLSN] = modifiedPaths
}

func (tx *Transaction) CreateDirectory(path string) error {
	return tx.applyUpdate(path, directoryNode)
}

func (tx *Transaction) Remove(path string) error {
	return tx.applyUpdate(path, negativeNode)
}

func (tx *Transaction) CreateFile(path string) error {
	return tx.applyUpdate(path, fileNode)
}

func (tx *Transaction) Read(path string) error {
	node, err := tx.findNode(path)
	if err != nil {
		return err
	}

	if node != nil && tx.readLSN < node.lsn {
		return newConflictingOperation(path, tx.readLSN, node.lsn)
	}

	return nil
}

type History struct {
	root       *node
	pathsByLSN map[storage.LSN]map[string]struct{}
	lsnByPath  map[string]storage.LSN
}

func New() *History {
	return &History{
		root:       newDirectory(),
		pathsByLSN: map[storage.LSN]map[string]struct{}{},
		lsnByPath:  map[string]storage.LSN{},
	}
}

func (h *History) Begin(readLSN storage.LSN) *Transaction {
	return &Transaction{
		history:       h,
		readLSN:       readLSN,
		root:          h.root.clone(),
		modifiedNodes: map[string]*node{},
	}
}

// Evict drops all changes related to the given LSN.
func (h *History) EvictLSN(lsn storage.LSN) {
	paths := h.pathsByLSN[lsn]
	// We're dropping the LSN and the associated reference
	// updates, so drop them from the indexes as well.
	delete(h.pathsByLSN, lsn)
	for path := range paths {
		delete(h.lsnByPath, path)
	}

	h.root.evict(lsn, paths)
}
