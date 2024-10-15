package gitaly

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/urfave/cli/v2"
	"gitlab.com/gitlab-org/gitaly/v16/internal/gitaly/storage/keyvalue"
	"gitlab.com/gitlab-org/gitaly/v16/internal/log"
	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

const (
	keyPartitionIDSeq      = `^partition_id_seq$`
	keyPartitionAssignment = `^partition_assignment/.+$`
	keyAppliedLSN          = `^(p/)(.{8})(/applied_lsn)$`
)

type decoder func([]byte) (string, error)

// keyFormatter defines the interface for formatting keys
type keyRegexFormatter interface {
	format(pattern *regexp.Regexp, key []byte) string
}

// decoders are used to decode the value of a key into a human-readable string.
var decoders = map[string]decoder{
	keyPartitionIDSeq:      decodeUint64("partition_id_seq"),
	keyPartitionAssignment: decodeUint64("partition_assignment"),
	keyAppliedLSN:          decodeProtoMessage(&gitalypb.LSN{}),
}

var formatters = map[string]keyRegexFormatter{
	keyAppliedLSN:          bigEndianUint64Formatter{},
	keyPartitionAssignment: stringFormatter{},
	keyPartitionIDSeq:      stringFormatter{},
}

type stringFormatter struct{}

func (f stringFormatter) format(_ *regexp.Regexp, key []byte) string {
	return string(key)
}

// bigEndianUint64Formatter converts keys to human-readable string
type bigEndianUint64Formatter struct{}

func (rf bigEndianUint64Formatter) format(pattern *regexp.Regexp, key []byte) string {
	matches := pattern.FindSubmatch(key)
	id := binary.BigEndian.Uint64(matches[2])

	return fmt.Sprintf("%s%d%s", matches[1], id, matches[3])
}

func databasePathFlag() *cli.StringFlag {
	return &cli.StringFlag{
		Name:     "db-path",
		Usage:    "Path to the badger database",
		Required: true,
	}
}

func newBadgerDBCmd() *cli.Command {
	cmd := &cli.Command{
		Name:        "db",
		Usage:       "Interact with the BadgerDB",
		UsageText:   "gitaly db <subcommand>",
		Description: "This command allows you to interact with the BadgerDB. It provides subcommands to list and get values from the database.",
		Subcommands: []*cli.Command{
			newBadgerDBListCmd(),
			newBadgerDBGetCmd(),
		},
	}
	return cmd
}

func newBadgerDBListCmd() *cli.Command {
	var formatKeys bool

	cmd := &cli.Command{
		Name:  "list",
		Usage: "List all keys in the BadgerDB, example usage: gitaly db list --db-path <db-path> ",
		Flags: []cli.Flag{
			databasePathFlag(),
			&cli.StringFlag{
				Name:     "prefix",
				Usage:    "Prefix to filter the keys, example usage: gitaly db list --db-path <db-path> --prefix p/\x00\x00\x00\x00\x00\x00\x00\x01/kv/raft",
				Required: false,
			},
			&cli.BoolFlag{
				Name:        "format-keys",
				Usage:       "Display the keys in human-readable format, example usage: gitaly db list --db-path <db-path> --format-keys",
				Destination: &formatKeys,
			},
		},
		Action: func(ctx *cli.Context) (returnErr error) {
			if ctx.NArg() > 0 {
				return fmt.Errorf("no arguments required, use -h for help")
			}

			db, err := openDatabase(ctx, ctx.String("db-path"))
			if err != nil {
				return fmt.Errorf("open database: %w", err)
			}

			defer func() {
				if err := db.Close(); err != nil {
					returnErr = errors.Join(returnErr, fmt.Errorf("closing database: %w", err))
				}
			}()
			return listKeys(ctx, db, ctx.String("prefix"), formatKeys)
		},
	}
	return cmd
}

func newBadgerDBGetCmd() *cli.Command {
	cmd := &cli.Command{
		Name:  "get",
		Usage: "Get a value of a key from the BadgerDB, example usage: gitaly db get --db-path <db-path> <raw-key>",
		Flags: []cli.Flag{
			databasePathFlag(),
		},
		ArgsUsage: "<key>",
		Action: func(ctx *cli.Context) (returnErr error) {
			if ctx.NArg() != 1 {
				return fmt.Errorf("exactly one argument required")
			}

			db, err := openDatabase(ctx, ctx.String("db-path"))
			if err != nil {
				return fmt.Errorf("open database: %w", err)
			}

			defer func() {
				if err := db.Close(); err != nil {
					returnErr = errors.Join(returnErr, fmt.Errorf("closing database: %w", err))
				}
			}()

			key := ctx.Args().First()
			return getValue(ctx, db, key)
		},
	}
	return cmd
}

func listKeys(ctx *cli.Context, db keyvalue.Store, prefix string, formatKeys bool) error {
	if prefix != "" {
		var err error
		// Unquote the prefix to handle escape sequences
		prefix, err = strconv.Unquote(`"` + prefix + `"`)
		if err != nil {
			return fmt.Errorf("convert prefix: %w", err)
		}
	}
	if err := db.View(func(txn keyvalue.ReadWriter) error {
		it := txn.NewIterator(keyvalue.IteratorOptions{Prefix: []byte(prefix)})
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if formatKeys {
				key = formatKey([]byte(key))
			}
			fmt.Fprintf(ctx.App.Writer, "%q\n", key)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("list keys: %w", err)
	}

	return nil
}

func getValue(ctx *cli.Context, db keyvalue.Store, key string) error {
	var value []byte
	var unquotedKey string
	if err := db.View(func(txn keyvalue.ReadWriter) error {
		// Unquote the key to handle escape sequences
		var err error
		unquotedKey, err = strconv.Unquote(`"` + key + `"`)
		if err != nil {
			return fmt.Errorf("unquote key: %w", err)
		}

		item, err := txn.Get([]byte(unquotedKey))
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(value)
		return err
	}); err != nil {
		return fmt.Errorf("retrieve value: %w", err)
	}

	decodedValue, err := decodeValue(ctx, unquotedKey, value)
	if err != nil {
		return fmt.Errorf("decode value: %w", err)
	}

	fmt.Fprintf(ctx.App.Writer, "%s\n", decodedValue)

	return nil
}

func decodeValue(ctx *cli.Context, key string, data []byte) (string, error) {
	for prefix, decode := range decoders {
		pattern := regexp.MustCompile(prefix)
		if pattern.MatchString(key) {
			return decode(data)
		}
	}

	fmt.Fprintf(ctx.App.ErrWriter, "no decoder found for key: %s\n", key)
	return hex.Dump(data), nil
}

func decodeUint64(label string) decoder {
	return func(data []byte) (string, error) {
		if len(data) != 8 {
			return "", fmt.Errorf("invalid length for uint64: %d", len(data))
		}
		value := binary.BigEndian.Uint64(data)
		return fmt.Sprintf("%s: %d", label, value), nil
	}
}

func decodeProtoMessage(msg proto.Message) decoder {
	return func(data []byte) (string, error) {
		if err := proto.Unmarshal(data, msg); err != nil {
			return "", fmt.Errorf("unmarshal %T: %w", msg, err)
		}
		return prototext.Format(msg), nil
	}
}

func openDatabase(ctx *cli.Context, storagePath string) (keyvalue.Store, error) {
	logger, err := log.Configure(ctx.App.ErrWriter, "json", "error")
	if err != nil {
		return nil, fmt.Errorf("configure logger: %w", err)
	}

	db, err := keyvalue.NewBadgerStore(logger, storagePath)
	if err != nil {
		return nil, fmt.Errorf("new badger store: %w", err)
	}

	return db, nil
}

// formatKey formats the key using the appropriate formatter
func formatKey(key []byte) string {
	for keyRegex, formatter := range formatters {
		pattern := regexp.MustCompile(keyRegex)
		if pattern.Match(key) {
			return formatter.format(pattern, key)
		}
	}
	return string(key)
}
