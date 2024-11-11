package fshistory

import (
	"fmt"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage"
)

// children contains child nodes of a node keyed by their
// path component.
type children map[string]*node

type nodeType int

const (
	uninitializedNode nodeType = iota
	// negativeNode represents a removed directory entry.
	negativeNode
	// directoryNode represents a directory.
	directoryNode
	// fileNode represents a file.
	fileNode
)

type node struct {
	// nodeType is the type of the node.
	nodeType nodeType
	// lsn stores the LSN this node was last modified at.
	lsn storage.LSN
	// children are the child nodes of this node.
	children children
	// knownDirectoryEntries counts the number of directory
	// entries this node has if it is a directory. Non-empty
	// directories can't be deleted.
	knownDirectoryEntries uint
}

// newNode returns a new node.
func newNode() *node {
	return &node{children: children{}}
}

func newDirectory() *node {
	n := newNode()
	n.nodeType = directoryNode
	return n
}

func (n *node) isDirectory() bool {
	return n.nodeType == directoryNode
}

// clone returns a deep copy of the node.
func (n *node) clone() *node {
	children := make(children, len(n.children))
	for key, child := range n.children {
		children[key] = child.clone()
	}

	return &node{
		nodeType:              n.nodeType,
		lsn:                   n.lsn,
		knownDirectoryEntries: n.knownDirectoryEntries,
		children:              children,
	}
}

func (n *node) isLive() bool {
	return n.nodeType != negativeNode
}

func (tx *Transaction) applyUpdate(path string, newType nodeType) error {
	var (
		pathPrefix string
		pathBase   = path
		parentNode *node
		node       = tx.root
	)

	// Walk down the tree until we find the node of the reference.
	for {
		prefix, suffix, foundSeparator := strings.Cut(pathBase, "/")
		if !foundSeparator {
			// If there was no separator, then it means the current node
			// is the parent node of the reference that is modified.
			//
			// Get the target node of the reference write.
			targetNode := node.children[pathBase]
			if targetNode == nil {
				// The target node didn't exist yet so create it.
				targetNode = newNode()
				node.children[pathBase] = targetNode
			}

			parentNode = node
			node = targetNode
			break
		}

		// Since there was a separator, we still need to walk down the tree
		// to find parent node.
		child := node.children[prefix]
		if child == nil {
			// Child node didn't exist. Create it so we can walk it further down.
			// The node must be a directory as otherwise the transaction wouldn't
			// attempt to operate in it.
			child = newDirectory()
			node.children[prefix] = child
			node.knownDirectoryEntries++
		}

		currentPath := filepath.Join(pathPrefix, prefix)
		if tx.readLSN < child.lsn {
			// If the child LSN is later than the read, it has been written after
			// our transaction started. This is a potential conflict.
			return newConflictingOperation(currentPath, tx.readLSN, child.lsn)
		} else if !child.isDirectory() {
			// This node was not a directory and can't be walked down.
			return newNotDirectoryError(currentPath)
		}

		parentNode = node
		node = child
		pathPrefix = currentPath
		pathBase = suffix
	}

	if tx.readLSN < node.lsn {
		return newConflictingOperation(path, tx.readLSN, node.lsn)
	}

	switch node.nodeType {
	case negativeNode:
		switch newType {
		case directoryNode, fileNode:
			node.nodeType = newType
			parentNode.knownDirectoryEntries++
		case negativeNode:
			return newNotFoundError(path)
		default:
			return fmt.Errorf("unhandled negative operation: %v", newType)
		}
	case directoryNode:
		switch newType {
		case negativeNode:
			// Check whether this node has directory entries. If so, it can't be deleted as
			// it is a non-empty directory.
			if node.knownDirectoryEntries > 0 {
				return newDirectoryNotEmptyError(path)
			}

			node.nodeType = newType
			parentNode.knownDirectoryEntries--
		case directoryNode, fileNode:
			return newAlreadyExistsError(path)
		default:
			return fmt.Errorf("unhandled directory operation: %v", newType)
		}
	case fileNode:
		switch newType {
		case negativeNode:
			node.nodeType = newType
			parentNode.knownDirectoryEntries--
		case directoryNode, fileNode:
			return newAlreadyExistsError(path)
		default:
			return fmt.Errorf("unhandled file operation: %v", newType)
		}
	case uninitializedNode:
		// This is a new node and the operation can't conflict.
		node.nodeType = newType
		if node.isLive() {
			parentNode.knownDirectoryEntries++
		}
	default:
		return fmt.Errorf("unhandled node type: %v", node.nodeType)
	}

	tx.modifiedNodes[path] = node

	return nil
}

func (tx *Transaction) findNode(path string) (*node, error) {
	var (
		pathPrefix string
		pathBase   = path
		node       = tx.root
	)

	// Walk down the tree until we find the node of the reference.
	for {
		prefix, suffix, foundSeparator := strings.Cut(pathBase, "/")
		if !foundSeparator {
			// If there was no separator, then it means the current node
			// is the parent node of the reference that is modified.
			//
			// Get the target node.
			return node.children[pathBase], nil
		}

		// Since there was a separator, we still need to walk down the tree
		// to find parent node.
		child := node.children[prefix]
		if child == nil {
			// Child node didn't exist. As a parent of the target node doesn't exist,
			// the target can't exist either.
			return nil, nil
		}

		currentPath := filepath.Join(pathPrefix, prefix)
		if tx.readLSN < child.lsn {
			// If the child LSN is later than the read, it has been written after
			// our transaction started. This is a potential conflict.
			return nil, newConflictingOperation(currentPath, tx.readLSN, child.lsn)
		} else if !child.isDirectory() {
			// This node was not a directory and can't be walked down.
			return nil, newNotDirectoryError(currentPath)
		}

		node = child
		pathPrefix = currentPath
		pathBase = suffix
	}
}

func (n *node) evict(evictedLSN storage.LSN, paths map[string]struct{}) {
	type pathElement struct {
		node  *node
		child string
	}

	var parentNodes []pathElement
	for path := range paths {
		var (
			pathPrefix  string
			pathBase    = path
			parentNodes = parentNodes[:0]
			node        = n
		)

		for {
			prefix, suffix, foundSeparator := strings.Cut(pathBase, "/")
			if !foundSeparator {
				// If there was no separator, then it means the current node
				// is the parent node of the reference being evicted.
				parentNodes = append(parentNodes, pathElement{node: node, child: prefix})
				node = node.children[pathBase]
				break
			}

			// Since there was a separator, we still need to walk down the tree
			// to find the parent node of the reference.
			parentNodes = append(parentNodes, pathElement{node: node, child: prefix})
			node = node.children[prefix]
			pathPrefix = filepath.Join(pathPrefix, prefix)
			pathBase = suffix
		}

		// Walk the hierarchy upwards from the first parent of the reference node.
		for i := len(parentNodes) - 1; i >= 0; i-- {
			path := parentNodes[i]

			child := path.node.children[path.child]
			// Check if the child node is still needed. The node can be evicted if it has no children and
			// its LSN is less than or equal to the evicted LSN. If the node has children, we'll need to
			// keep until the children are evicted. We'll evict the parent when the last child is evicted.
			// If the node's LSN is later than the evicted LSN, the node is still needed for conflict checking
			// and can't be evicted. Keep the node and its parents.
			if len(child.children) > 0 || evictedLSN < child.lsn {
				break
			}

			if child.isLive() {
				// If the dropped node is not a negative, update the counter in the parent nodes to
				// reflect the removal of a live inode below them.
				path.node.knownDirectoryEntries--
			}

			// This node was unneeded, remove it.
			delete(path.node.children, path.child)
		}
	}
}
