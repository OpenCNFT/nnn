//go:build !gitaly_test_sha256

package git_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
)

func TestObjectHash_ValidateHex(t *testing.T) {
	for _, hash := range []struct {
		desc     string
		hash     git.ObjectHash
		validHex string
	}{
		{
			desc:     "SHA1",
			hash:     git.ObjectHashSHA1,
			validHex: "356e7793f9654d51dfb27312a1464062bceb9fa3",
		},
		{
			desc:     "SHA256",
			hash:     git.ObjectHashSHA256,
			validHex: "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f",
		},
	} {
		t.Run(hash.desc, func(t *testing.T) {
			for _, tc := range []struct {
				desc  string
				hex   string
				valid bool
			}{
				{
					desc:  "valid object ID",
					hex:   hash.validHex,
					valid: true,
				},
				{
					desc:  "object ID with non-hex characters fails",
					hex:   "x" + hash.validHex[1:],
					valid: false,
				},
				{
					desc:  "object ID with upper-case letters fails",
					hex:   strings.ToUpper(hash.validHex),
					valid: false,
				},
				{
					desc:  "too short object ID fails",
					hex:   hash.validHex[:len(hash.validHex)-1],
					valid: false,
				},
				{
					desc:  "too long object ID fails",
					hex:   hash.validHex + "3",
					valid: false,
				},
				{
					desc:  "empty string fails",
					hex:   "",
					valid: false,
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					err := hash.hash.ValidateHex(tc.hex)
					if tc.valid {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
						require.EqualError(t, err, fmt.Sprintf("invalid object ID: %q", tc.hex))
					}
				})
			}
		})
	}
}

func TestObjectHash_FromHex(t *testing.T) {
	for _, hash := range []struct {
		desc     string
		hash     git.ObjectHash
		validHex string
	}{
		{
			desc:     "SHA1",
			hash:     git.ObjectHashSHA1,
			validHex: "356e7793f9654d51dfb27312a1464062bceb9fa3",
		},
		{
			desc:     "SHA256",
			hash:     git.ObjectHashSHA256,
			validHex: "aec070645fe53ee3b3763059376134f058cc337247c978add178b6ccdfb0019f",
		},
	} {
		t.Run(hash.desc, func(t *testing.T) {
			for _, tc := range []struct {
				desc  string
				hex   string
				valid bool
			}{
				{
					desc:  "valid object ID",
					hex:   hash.validHex,
					valid: true,
				},
				{
					desc:  "object ID with non-hex characters fails",
					hex:   "x" + hash.validHex[1:],
					valid: false,
				},
				{
					desc:  "object ID with upper-case letters fails",
					hex:   strings.ToUpper(hash.validHex),
					valid: false,
				},
				{
					desc:  "too short object ID fails",
					hex:   hash.validHex[:len(hash.validHex)-1],
					valid: false,
				},
				{
					desc:  "too long object ID fails",
					hex:   hash.validHex + "3",
					valid: false,
				},
				{
					desc:  "empty string fails",
					hex:   "",
					valid: false,
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					oid, err := hash.hash.FromHex(tc.hex)
					if tc.valid {
						require.NoError(t, err)
						require.Equal(t, tc.hex, oid.String())
					} else {
						require.Error(t, err)
					}
				})
			}
		})
	}
}

func TestObjectID_Bytes(t *testing.T) {
	for _, tc := range []struct {
		desc          string
		oid           git.ObjectID
		expectedBytes []byte
		expectedErr   error
	}{
		{
			desc:          "zero OID",
			oid:           git.ObjectHashSHA1.ZeroOID,
			expectedBytes: bytes.Repeat([]byte{0}, 20),
		},
		{
			desc:          "valid object ID",
			oid:           git.ObjectID(strings.Repeat("8", 40)),
			expectedBytes: bytes.Repeat([]byte{0x88}, 20),
		},
		{
			desc:        "invalid object ID",
			oid:         git.ObjectID(strings.Repeat("8", 39) + "x"),
			expectedErr: hex.InvalidByteError('x'),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actualBytes, err := tc.oid.Bytes()
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedBytes, actualBytes)
		})
	}
}

func TestObjectHash_IsZeroOID(t *testing.T) {
	for _, hash := range []struct {
		desc     string
		hash     git.ObjectHash
		validHex string
	}{
		{
			desc: "SHA1",
			hash: git.ObjectHashSHA1,
		},
		{
			desc: "SHA256",
			hash: git.ObjectHashSHA256,
		},
	} {
		t.Run(hash.desc, func(t *testing.T) {
			for _, tc := range []struct {
				desc   string
				oid    git.ObjectID
				isZero bool
			}{
				{
					desc:   "zero object ID",
					oid:    hash.hash.ZeroOID,
					isZero: true,
				},
				{
					desc:   "zero object ID",
					oid:    hash.hash.EmptyTreeOID,
					isZero: false,
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					require.Equal(t, tc.isZero, hash.hash.IsZeroOID(tc.oid))
				})
			}
		})
	}
}
