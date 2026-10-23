package providerconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
)

// Verify checks the configured file and its pinned digest without executing it.
// A matching digest verifies integrity, not the publisher's authenticity.
func (i Installation) Verify(engine string) error {
	if i.Disabled {
		return i.Diagnostic(engine, "provider_disabled", "The configured provider is disabled.")
	}
	file, err := os.Open(i.Executable)
	if errors.Is(err, os.ErrNotExist) {
		return i.Diagnostic(engine, "provider_executable_missing", "The "+i.origin.label()+" provider executable was not found; "+i.origin.restore(engine)+".")
	}
	if err != nil {
		return i.Diagnostic(engine, "provider_unreadable", "The "+i.origin.label()+" provider executable cannot be read; check its file permissions.")
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return i.Diagnostic(engine, "provider_unreadable", "The "+i.origin.label()+" provider executable must be a readable regular file.")
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return i.Diagnostic(engine, "provider_unreadable", "The "+i.origin.label()+" provider executable could not be read completely.")
	}
	if !strings.EqualFold(hex.EncodeToString(digest.Sum(nil)), i.SHA256) {
		return i.Diagnostic(engine, "provider_untrusted", i.origin.untrusted(engine))
	}
	return nil
}
