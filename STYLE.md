# Gitaly code style

## Character set

### Avoid non-ASCII characters in developer-facing code

Code that is developer-facing only, like variables, functions or test
descriptions should use the ASCII character set only. This is to ensure that
code is accessible to different developers with varying setups.

## Errors

### Use %w when wrapping errors

Use `%w` when wrapping errors with context.

```go
fmt.Errorf("foo context: %w", err)
```

It allows to inspect the wrapped error by the caller with [`errors.As`](https://golang.org/pkg/errors/#As) and [`errors.Is`](https://golang.org/pkg/errors/#Is). More info about `errors` package capabilities could be found in the [blog post](https://blog.golang.org/go1.13-errors).

### Keep errors short

It is customary in Go to pass errors up the call stack and decorate
them. To be a good neighbor to the rest of the call stack we should keep
our errors short.

```go
// Good
fmt.Errorf("peek diff line: %w", err)

// Too long
fmt.Errorf("ParseDiffOutput: Unexpected error while peeking: %w", err)
```

### Use lower case in errors

Use lower case in errors; it is OK to preserve upper case in names.

### Errors should stick to the facts

It is tempting to write errors that explain the problem that occurred.
This can be appropriate in some end-user facing situations, but it is
never appropriate for internal error messages. When your
interpretation is wrong it puts the reader on the wrong track.

Stick to the facts. Often it is enough to just describe in a few words
what we were trying to do.

### Use %q when interpolating strings

Unless it would lead to incorrect results, always use `%q` when
interpolating strings. The `%q` operator quotes strings and escapes
spaces and non-printable characters. This can save a lot of debugging
time.

### Use [`structerr`](internal/structerr/error.go) for error creation

Gitaly uses its own small error framework that can be used to generate
errors with the `structerr` package. This package handles:

- Propagation of the correct [gRPC error code](https://grpc.github.io/grpc/core/md_doc_statuscodes.html)
  so that clients can see a rough classification of why an error happens.
- Handling of error metadata that makes details about the error's context
  available in logs.
- Handling of the extended gRPC error model by attaching structured Protobuf
  message to an error.

Therefore, `structerr` is recommended for generating errors in Gitaly, both:

- In the context of gRPC services.
- For low-level code.

The following example shows how the `structerr` package can be used:

```golang
func CheckFrobnicated(s string) error {
    if s != "frobnicated" {
        return structerr.NewInvalidArgument("input is not frobnicated").
            WithMetadata("input", s).
            WithDetail(&gitalypb.NotFrobnicatedError{
                Input: s,
            })
    }

    return nil
}
```

Unless the root cause of an error can be determined and categorized, you should
use `structerr.New()` to generate an error with an `Unknown` error code. This
code will automatically get overridden when the error is wrapped with a specific
error code.

### Error wrapping

You should use wrapping directives `"%w"` to wrap errors. This
causes the `structerr` package to:

- Propagate the gRPC error in case the wrapped error already contains an error
  code.
- Merge metadata of any wrapped `structerr` with the newly created error's
  metadata, if any.
- Merge error details so that the returned gRPC error has all structured
  errors attached to it.

For example:

```golang
func ValidateArguments(s string) error {
    if err := CheckFrobnicated(s); err != nil {
        return structerr.ErrInvalidArgumentf("checking frobnication: %w", err)
    }

    return nil
}
```

### Error metadata

Error metadata attaches dynamic data to errors that give the
consumer of logs additional context around why an error has happened. This can
include:

- Parameters controlled by the caller, assuming they don't leak any secrets,
  like an object ID.
- The standard error output of a command spawned by Gitaly.

Attaching such data as metadata is recommended over embedding it into the error
message. This makes errors easier to follow and allows us to deduplicate errors
by their now-static message in tools like Sentry.

### Error details

By default, the gRPC framework only propagates an error code and the error
message to clients. This information is inadequate for a client to decide how a
specific error should be handled:

- Error codes are too broad, so different errors end up with the same code.
- Parsing error messages is fragile and heavily discouraged.

The gRPC framework allows for a [richer error model](https://grpc.io/docs/guides/error/#richer-error-model).
With this error model, the server can attach structured Protobuf messages to
errors returned to the client. These Protobuf messages can be extracted on the
client side to allow more fine-grained error handling. Error details should be
added to an error when you know that the client side needs to base its
behavior on the specific error that has occurred.

For an RPC `Frobnicate()`, the error details should be of the message type
`FrobnicateError`. The different error cases should then be wrapped into a
`oneof` field. For example:

```proto
service FrobnicationService {
    rpc Frobnicate(FrobnicateRequest) returns (FrobnicateResponse);
}

message FrobnicateError {
  oneof error {
    VerificationError verification_error = 1;
  }
}
```

### Unavailable code

The Unavailable status code is reserved for cases that clients are encouraged to retry. The most suitable use cases for
this status code are in interceptors or network-related components such as load-balancing. The official documentation
differentiates the usage of status codes as the following:

> (a) Use UNAVAILABLE if the client can retry just the failing call.
> (b) Use ABORTED if the client should retry at a higher level
> (c) Use FAILED_PRECONDITION if the client should not retry until the
> system state has been explicitly fixed

In the past we've had multiple places in the source code where an error from sending a streaming message was captured
and wrapped in an `Unavailable` code. This status code is often not correct because it can raise other less common
errors, such as buffer overflow (`ResourceExhausted`), max message size exceeded (`ResourceExhausted`), or encoding
failure (`Internal`). It's more appropriate for the handler to propagate the error up as an `Aborted` status code.

Another common misused pattern is wrapping the spawned process exit code. In many cases, if Gitaly can intercept the
exit code or/and error from stderr, it must have a precise error code (`InvalidArgument`, `NotFound`, `Internal`).
However, Git processes may exit with 128 status code and un-parseable stderr. We can intercept it as an operation was
rejected because the system is not in a state where it can be executed (resource inaccessible, invalid refs, etc.).
For these situations, `FailedPrecondition` is the most suitable choice.

Thus, gRPC handlers should avoid using `Unavailable` status code.

## Logging

### Use context-based logging

The context may contain additional information about the current calling context, like for example the correlation ID.
This structured data can be added to the context via calls to `log.AddFields()` and is injected by default via our
requestinfo gRPC middleware.

This structured data can be extracted from the context into generated log message by using context-aware logging
functions like `log.DebugContext()` and related functions. These functions should thus be used instead of the
non-context-aware logging functions like `log.Debug()` whenever a context is available.

### Errors

When logging an error, use the `WithError(err)` method.

### Use the `log.Logger` interface

In case you want to pass around the logger, use the `log.Logger`
interface instead of either `*logrus.Entry` or `*logrus.Logger`.

### Use snake case for fields

When writing log entries, you should use `logger.WithFields()` to add relevant
metadata relevant to the entry. The keys should use snake case:

```golang
logger.WithField("correlation_id", 12345).Info("StartTransaction")
```

### Use RPC name as log message

In case you do not want to write a specific log message, but only want to notify
about a certain function or RPC being called, you should use the function's name
as the log message:

```golang
func StartTransaction(id uint64) {
    logger.WithField("transaction_id", id).Debug("StartTransaction")
}
```

### Embed package into log entries

In order to associate log entries with a given code package, you should add a
`component` field to the log entry. If the log entry is generated in a method,
the component should be `$PACKAGE_NAME.$STRUCT_NAME`:

```golang
package transaction

type Manager struct {}

func (m Manager) StartTransaction(ctx context.Context) {
    log.FromContext(ctx).WithFields(log.Fields{
        "component": "transaction.Manager",
    }).Debug("StartTransaction")
}
```

## Literals and constructors

### Use "address of struct" instead of new

The following are equivalent in Go:

```golang
// Preferred
foo := &Foo{}

// Don't use
foo := new(Foo)
```

There is no strong reason to prefer one over the other. But mixing
them is unnecessary. We prefer the first style.

### Use hexadecimal byte literals in strings

Sometimes you want to use a byte literal in a Go string. Use a
hexadecimal literal in that case. Unless you have a good reason to use
octal of course.

```golang
// Preferred
foo := "bar\x00baz"

// Don't use octal
foo := "bar\000baz"
```

Octal has the bad property that to represent high bytes, you need 3
digits, but then you may not use a `4` as the first digit. 0377 equals
255 which is a valid byte value. 0400 equals 256 which is not. With
hexadecimal you cannot make this mistake because the largest two digit
hex number is 0xff which equals 255.

## Functions

### Method Receivers

Without any good reason, methods should always use value receivers, where good
reasons include (but are not limited to) performance/memory concerns or
modification of state in the receiver. Otherwise, if any of the type's methods
requires a pointer receiver, all methods should be pointer receivers.

### Don't use "naked return"

In a function with named return variables it is valid to have a plain
("naked") `return` statement, which will return the named return
variables.

In Gitaly we don't use this feature. If the function returns one or
more values, then always pass them to `return`.

## Ordering

### Declare types before their first use

A type should be declared before its first use.

## Tests

### Naming

Prefer to name tests in the same style as [examples](https://golang.org/pkg/testing/#hdr-Examples).

To declare a test for the package, a function F, a type T and method M on type T are:

```go
func TestF() { ... }
func TestT() { ... }
func TestT_M() { ... }
```

A suffix may be appended to distinguish between test cases. The suffix must
start with a lower-case letter and use camelCasing to separate words.

```go
func TestF_suffix() { ... }
func TestT_suffix() { ... }
func TestT_M_suffix() { ... }
func TestT_M_suffixWithMultipleWords() { ... }
```

### Test helpers

Helper functions for test helpers should be clearly marked with `t.Helper()` so
that stack traces become more usable. `testing.TB` arguments should always be
passed as first parameter, followed by `context.Context` if required.

```go
func testHelper(tb testing.TB, ctx context.Context) {
    tb.Helper()
    ...
}
```

### Table-driven tests

We like table-driven tests ([Table-driven tests using subtests](https://blog.golang.org/subtests#TOC_4.), [Cheney blog post], [Golang wiki]).

- Use [subtests](https://blog.golang.org/subtests#TOC_4.) with your table-driven tests, using `t.Run`:

```go
func TestTime(t *testing.T) {
    testCases := []struct {
        gmt  string
        loc  string
        want string
    }{
        {"12:31", "Europe/Zuri", "13:31"},
        {"12:31", "America/New_York", "7:31"},
        {"08:08", "Australia/Sydney", "18:08"},
    }
    for _, tc := range testCases {
        t.Run(fmt.Sprintf("%s in %s", tc.gmt, tc.loc), func(t *testing.T) {
            loc, err := time.LoadLocation(tc.loc)
            if err != nil {
                t.Fatal("could not load location")
            }
            gmt, _ := time.Parse("15:04", tc.gmt)
            if got := gmt.In(loc).Format("15:04"); got != tc.want {
                t.Errorf("got %s; want %s", got, tc.want)
            }
        })
    }
}
```

  [Cheney blog post]: https://dave.cheney.net/2013/06/09/writing-table-driven-tests-in-go
  [Golang wiki]: https://github.com/golang/go/wiki/TableDrivenTests

### Fatal exit

Aborting test execution with any function which directly or indirectly calls
`os.Exit()` should be avoided as this will cause any deferred function calls to
not be executed. As a result, tests may leave behind testing state. Most
importantly, this includes any calls to `log.Fatal()` and related functions.

### Common setup

The `TestMain()` function shouldn't do any package-specific setup. Instead, all
tests are supposed to set up required state as part of the tests themselves. All
`TestMain()` functions must call `testhelper.Run()` though, which performs the
setup of global state required for tests.

### Test data

Tests should not rely on static Git data but instead generate test data at
runtime if possible. This is done so that we can easily adapt to changes in
Git's repository or object format, e.g. in the transition from the SHA1 to the
SHA256 object hash. Using seed repositories is thus discouraged.

As an alternative, tests can rely on the following helper functions to generate
their test data:

- `gittest.WriteCommit()`
- `gittest.WriteTree()`
- `gittest.WriteBlob()`
- `gittest.WriteTag()`

## Black box and white box testing

The dominant style of testing in Gitaly is "white box" testing, meaning
test functions for package `foo` declare their own package also to be
`package foo`. This gives the test code access to package internals. Go
also provides a mechanism sometimes called "black box" testing where the
test functions are not part of the package under test: you write
`package foo_test` instead. Depending on your point of view, the lack of
access to package internals when using black-box is either a bug or a
feature.

As a team we are currently divided on which style to prefer so we are
going to allow both. In areas of the code where there is a clear
pattern, please stick with the pattern. For example, almost all our
service tests are white box.

## Prometheus metrics

Prometheus is a great tool to collect data about how our code behaves in
production. When adding new Prometheus metrics, please follow the [best
practices](https://prometheus.io/docs/practices/naming/) and be aware of
the
[gotchas](https://prometheus.io/docs/practices/instrumentation/#things-to-watch-out-for).

### Main function

If tests require a `TestMain()` function for common setup, this function should
be implemented in a file called `testhelper_test.go`

## Git Commands

Gitaly relies heavily on spawning Git subprocesses to perform work. Git commands are spawned by using the Git command
factory at [`internal/git/gitcmd/command_factory.go`](internal/git/gitcmd/command_factory.go). Proper usage is important to
mitigate these injection risks:

- When toggling an option, prefer a longer flag over a short flag for
  readability.
  - Desired: `gitcmd.Flag{Name: "--long-flag"}` is easier to read and audit
  - Undesired: `gitcmd.Flag{Name: "-L"}`
- When providing a variable to configure a flag, make sure to include the
  variable after an equal sign
  - Desired: `[]gitcmd.Flag{Name: "-a="+foo}` prevents flag injection
  - Undesired: `[]gitcmd.Flag(Name: "-a"+foo)` allows flag injection
- Always define a flag's name via a constant, never use a variable:
  - Desired: `[]gitcmd.Flag{Name: "-a"}`
  - Undesired: `[]gitcmd.Flag{Name: foo}` is ambiguous and difficult to audit

## Go Imports Style

When adding new package dependencies to a source code file, keep all standard
library packages in one contiguous import block, and all third party packages
(which includes Gitaly packages) in another contiguous block. This way, the
goimports tool will deterministically sort the packages which reduces the noise
in reviews.

Example of **valid** usage:

```go
import (
    "context"
    "io"
    "os/exec"

    "gitlab.com/gitlab-org/gitaly/internal/command"
    "gitlab.com/gitlab-org/gitaly/internal/git/alternates"
    "gitlab.com/gitlab-org/gitaly/internal/git/repository"
)
```

Example of **invalid** usage:

```go
import (
    "io"
    "os/exec"

    "context"

    "gitlab.com/gitlab-org/gitaly/internal/git/alternates"
    "gitlab.com/gitlab-org/gitaly/internal/git/repository"

    "gitlab.com/gitlab-org/gitaly/internal/command"
)
```

## Goroutine Guidelines

Gitaly is a long lived process. This means that every goroutine spawned carries
liability until either the goroutine ends or the program exits. Some goroutines
are expected to run until program termination (e.g. server listeners and file
walkers). However, the vast majority of goroutines spawned are in response to
an RPC, and in most cases should end before the RPC returns. Proper cleanup of
goroutines is crucial to prevent leaks. When in doubt, you can consult the
following guide:

### Is A Goroutine Necessary?

Avoid using goroutines if the job at hand can be done just as easily and just as well without them.

### Background Task Goroutines

These are goroutines we expect to run the entire life of the process. If they
crash, we expect them to be restarted. If they restart often, we may want a way
to delay subsequent restarts to prevent resource consumption. See
[`dontpanic.GoForever`] for a useful function to handle goroutine restarts with
Sentry observability.

### RPC Goroutines

These are goroutines created to help handle an RPC. A goroutine that is started
during an RPC will also need to end when the RPC completes. This quality makes
it easy to reason about goroutine cleanup.

#### Defer-based Cleanup

One of the safest ways to clean up goroutines (as well as other resources) is
via deferred statements. For example:

```go
func (scs SuperCoolService) MyAwesomeRPC(ctx context.Context, r Request) error {
    done := make(chan struct{}) // signals the goroutine is done
    defer func() { <-done }() // wait until the goroutine is done

    go func() {
        defer close(done)    // signal when the goroutine returns
    doWork(r)
    }()

    return nil
}
```

Note the heavy usage of defer statements. Using defer statements means that
clean up will occur even if a panic bubbles up the call stack (**IMPORTANT**).
Also, the resource cleanup will
occur in a predictable manner since each defer statement is pushed onto a LIFO
stack of defers. Once the function ends, they are popped off one by one.

### Goroutine Panic Risks

Additionally, every new goroutine has the potential to crash the process. Any
unrecovered panic can cause the entire process to crash and take out any in-
flight requests (**VERY BAD**). When writing code that creates a goroutine,
consider the following question: How confident are you that the code in the
goroutine won't panic? If you can't answer confidently, you may want to use a
helper function to handle panic recovery: [`dontpanic.Go`].

### Limiting Goroutines

When spawning goroutines, you should always be aware of how many goroutines you
will be creating. While cheap, goroutines are not free. Consult the following
questions if you need help deciding if goroutines are being improperly used:

1. How many goroutines will it take the task/RPC to complete?
   - Fixed number - 👍 Good
   - Variable number - 👇 See next question...
1. Does the goroutine count scale with a configuration value (e.g. storage
   locations or concurrency limit)?
   - Yes - 👍 Good
   - No - 🚩 this is a red flag! An RPC where the goroutines do not scale
     predictably will open up the service to denial of service attacks.

[`dontpanic.GoForever`]: https://pkg.go.dev/gitlab.com/gitlab-org/gitaly/internal/dontpanic?tab=doc#GoForever
[`dontpanic.Go`]: https://pkg.go.dev/gitlab.com/gitlab-org/gitaly/internal/dontpanic?tab=doc#Go
