package providerrelease_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerrelease"
)

const (
	digestA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	digestB = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
)

func TestChecksum_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		checksums string
		want      string
	}{
		{"text mode", digestA + "  archive.tar.gz\n" + digestB + "  ptah-provider.json\n", digestA},
		{"binary mode", digestA + " *archive.tar.gz\n", digestA},
		{"uppercase digest", strings.ToUpper(digestA) + "  archive.tar.gz\n", digestA},
		{"carriage returns", digestA + "  archive.tar.gz\r\n", digestA},
		{"a longer name is another file", digestB + "  archive.tar.gz.sbom.json\n" + digestA + "  archive.tar.gz\n", digestA},
		{"a bad line for another file", "garbage\n" + digestA + "  archive.tar.gz\n", digestA},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := providerrelease.Checksum([]byte(test.checksums), "archive.tar.gz")
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestChecksum_FailurePath_Missing(t *testing.T) {
	for _, test := range []struct{ name, checksums string }{
		{"empty file", ""},
		{"other files only", digestA + "  archive.tar.gz.sbom.json\n" + digestB + "  ptah-provider.json\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := providerrelease.Checksum([]byte(test.checksums), "archive.tar.gz")
			c.Assert(err, qt.ErrorIs, providerrelease.ErrChecksumMissing)
			c.Assert(err, qt.ErrorMatches, `checksums.txt has no entry for archive.tar.gz`)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestChecksum_FailurePath_Invalid(t *testing.T) {
	for _, test := range []struct{ name, checksums, wantErr string }{
		{"empty digest", "  archive.tar.gz\n", `.*the digest of archive.tar.gz is "", not a SHA-256`},
		{"short digest", "abc123  archive.tar.gz\n", `.*the digest of archive.tar.gz is "abc123", not a SHA-256`},
		{"not hexadecimal", strings.Repeat("z", 64) + "  archive.tar.gz\n", `.*not a SHA-256`},
		{"listed twice", digestA + "  archive.tar.gz\n" + digestA + "  archive.tar.gz\n", `.*archive.tar.gz is listed 2 times`},
		{"too large", strings.Repeat("x", providerrelease.MaxChecksumsSize+1), `.*larger than 1048576 bytes`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := providerrelease.Checksum([]byte(test.checksums), "archive.tar.gz")
			c.Assert(err, qt.ErrorIs, providerrelease.ErrChecksumInvalid)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}
