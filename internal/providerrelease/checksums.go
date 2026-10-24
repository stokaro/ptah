package providerrelease

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strings"
)

// ChecksumsName is the release asset that lists the SHA-256 digest of every
// other asset.
const ChecksumsName = "checksums.txt"

// MaxChecksumsSize bounds the bytes read for one checksums.txt.
const MaxChecksumsSize = 1 << 20

var (
	// ErrChecksumMissing reports a checksums.txt with no entry for a file.
	ErrChecksumMissing = errors.New("checksums.txt has no entry")
	// ErrChecksumInvalid reports an entry whose digest is empty or is not a
	// SHA-256, or a file listed more than once.
	ErrChecksumInvalid = errors.New("checksums.txt entry is invalid")
)

// Checksum returns the SHA-256 digest that checksums.txt lists for name, in
// lowercase hexadecimal. checksums uses the sha256sum format: one
// "<hex digest>  <file name>" per line, with an optional "*" before the name.
//
// A name with no line is [ErrChecksumMissing]. A name listed twice, or listed
// with an empty or malformed digest, is [ErrChecksumInvalid]. Lines for other
// files are not judged.
func Checksum(checksums []byte, name string) (string, error) {
	if len(checksums) > MaxChecksumsSize {
		return "", fmt.Errorf("%w: checksums.txt is larger than %d bytes", ErrChecksumInvalid, MaxChecksumsSize)
	}
	var digests []string
	scanner := bufio.NewScanner(bytes.NewReader(checksums))
	scanner.Buffer(make([]byte, 0, 4096), MaxChecksumsSize)
	for scanner.Scan() {
		digest, file, found := entry(scanner.Text())
		if found && file == name {
			digests = append(digests, digest)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrChecksumInvalid, err)
	}
	switch {
	case len(digests) == 0:
		return "", fmt.Errorf("%w for %s", ErrChecksumMissing, name)
	case len(digests) > 1:
		return "", fmt.Errorf("%w: %s is listed %d times", ErrChecksumInvalid, name, len(digests))
	case !ValidDigest(digests[0]):
		return "", fmt.Errorf("%w: the digest of %s is %q, not a SHA-256", ErrChecksumInvalid, name, digests[0])
	}
	return strings.ToLower(digests[0]), nil
}

// entry splits one line into its digest and file name. A line with a single
// field is a file name with an empty digest, so an entry whose digest is
// missing is reported as invalid rather than as absent.
func entry(line string) (digest, file string, found bool) {
	fields := strings.Fields(line)
	switch len(fields) {
	case 0:
		return "", "", false
	case 1:
		return "", strings.TrimPrefix(fields[0], "*"), true
	default:
		rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), fields[0]))
		return fields[0], strings.TrimPrefix(rest, "*"), true
	}
}

// ValidDigest reports whether s is a hexadecimal SHA-256 digest, in either
// case.
func ValidDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if !strings.ContainsRune("0123456789abcdefABCDEF", r) {
			return false
		}
	}
	return true
}
