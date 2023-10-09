package storagemgr

import (
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// hookFunc is a function that is executed at a specific point. It gets a hookContext that allows it to
// influence the execution of the test.
type hookFunc func(hookContext)

// hookContext are the control toggels available in a hook.
type hookContext struct {
	// closeManager calls the calls Close on the TransactionManager.
	closeManager func()
	// database provides access to the database for the hook handler.
	database *badger.DB
	tb       testing.TB
}

// hooks are functions that get invoked at specific points of the TransactionManager Run method. They allow
// for hooking into the Run method at specific poins which would otherwise to do assertions that would otherwise
// not be possible.
type hooks struct {
	// beforeReadLogEntry is invoked before a log entry is read from the database.
	beforeReadLogEntry hookFunc
	// beforeStoreLogEntry is invoked before the log entry is stored to the database.
	beforeStoreLogEntry hookFunc
	// beforeDeferredClose is invoked before the deferred Close is invoked in Run.
	beforeDeferredClose hookFunc
	// beforeDeleteLogEntry is invoked before a log entry is deleted from the database.
	beforeDeleteLogEntry hookFunc
	// beforeReadAppliedLogIndex is invoked before the applied log index is read from the database.
	beforeReadAppliedLogIndex hookFunc
	// beforeStoreAppliedLogIndex is invoked before a the applied log index is stored.
	beforeStoreAppliedLogIndex hookFunc
}

// installHooks installs the configured hooks into the transactionManager.
func installHooks(tb testing.TB, transactionManager *TransactionManager, database *badger.DB, hooks hooks) {
	hookContext := hookContext{closeManager: transactionManager.close, database: database, tb: &testingHook{TB: tb}}

	transactionManager.close = func() {
		programCounter, _, _, ok := runtime.Caller(2)
		require.True(tb, ok)

		isDeferredCloseInRun := strings.HasSuffix(
			runtime.FuncForPC(programCounter).Name(),
			"gitaly.(*TransactionManager).Run",
		)

		if isDeferredCloseInRun && hooks.beforeDeferredClose != nil {
			hooks.beforeDeferredClose(hookContext)
		}

		hookContext.closeManager()
	}

	transactionManager.db = databaseHook{
		database:    newDatabaseAdapter(database),
		hooks:       hooks,
		hookContext: hookContext,
	}
}

type databaseHook struct {
	database
	hookContext
	hooks
}

func (hook databaseHook) View(handler func(databaseTransaction) error) error {
	return hook.database.View(func(transaction databaseTransaction) error {
		return handler(databaseTransactionHook{
			databaseTransaction: transaction,
			hookContext:         hook.hookContext,
			hooks:               hook.hooks,
		})
	})
}

func (hook databaseHook) Update(handler func(databaseTransaction) error) error {
	return hook.database.Update(func(transaction databaseTransaction) error {
		return handler(databaseTransactionHook{
			databaseTransaction: transaction,
			hookContext:         hook.hookContext,
			hooks:               hook.hooks,
		})
	})
}

func (hook databaseHook) NewWriteBatch() writeBatch {
	return writeBatchHook{
		writeBatch:  hook.database.NewWriteBatch(),
		hookContext: hook.hookContext,
		hooks:       hook.hooks,
	}
}

type databaseTransactionHook struct {
	databaseTransaction
	hookContext
	hooks
}

var (
	regexLogEntry = regexp.MustCompile("partition/.+/log/entry/")
	regexLogIndex = regexp.MustCompile("partition/.+/log/index/applied")
)

func (hook databaseTransactionHook) Get(key []byte) (*badger.Item, error) {
	if regexLogEntry.Match(key) {
		if hook.hooks.beforeReadLogEntry != nil {
			hook.hooks.beforeReadLogEntry(hook.hookContext)
		}
	} else if regexLogIndex.Match(key) {
		if hook.hooks.beforeReadAppliedLogIndex != nil {
			hook.hooks.beforeReadAppliedLogIndex(hook.hookContext)
		}
	}

	return hook.databaseTransaction.Get(key)
}

func (hook databaseTransactionHook) NewIterator(options badger.IteratorOptions) *badger.Iterator {
	return hook.databaseTransaction.NewIterator(options)
}

func (hook databaseTransactionHook) Delete(key []byte) error {
	if regexLogEntry.Match(key) && hook.beforeDeleteLogEntry != nil {
		hook.beforeDeleteLogEntry(hook.hookContext)
	}

	return hook.databaseTransaction.Delete(key)
}

type writeBatchHook struct {
	writeBatch
	hookContext
	hooks
}

func (hook writeBatchHook) Set(key []byte, value []byte) error {
	if regexLogIndex.Match(key) && hook.hooks.beforeStoreAppliedLogIndex != nil {
		hook.hooks.beforeStoreAppliedLogIndex(hook.hookContext)
	}

	if regexLogEntry.Match(key) && hook.hooks.beforeStoreLogEntry != nil {
		hook.hooks.beforeStoreLogEntry(hook.hookContext)
	}

	return hook.writeBatch.Set(key, value)
}

func (hook writeBatchHook) Flush() error { return hook.writeBatch.Flush() }

func (hook writeBatchHook) Cancel() { hook.writeBatch.Cancel() }

type testingHook struct {
	testing.TB
}

// We override the FailNow call to the regular testing.Fail, so that it can be
// used within goroutines without replacing calls made to the `require` library.
func (t testingHook) FailNow() {
	t.Fail()
}
