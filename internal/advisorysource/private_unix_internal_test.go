//go:build unix

package advisorysource

// White-box testing required: a cache file another user owns cannot be made
// without a second account, so the owner check is driven with a FileInfo
// whose owner differs from the user running the test.

import (
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// ownedInfo is a private regular file owned by uid.
type ownedInfo struct{ uid uint32 }

func (ownedInfo) Name() string       { return "entry.json" }
func (ownedInfo) Size() int64        { return 0 }
func (ownedInfo) Mode() fs.FileMode  { return 0o600 }
func (ownedInfo) ModTime() time.Time { return time.Time{} }
func (ownedInfo) IsDir() bool        { return false }
func (i ownedInfo) Sys() any         { return &syscall.Stat_t{Uid: i.uid} }

func TestCheckPrivate_Owner(t *testing.T) {
	c := qt.New(t)
	self := uint32(os.Geteuid()) // #nosec G115 -- a user ID fits in the uint32 a Stat_t carries

	c.Assert(checkPrivate("cache/entry.json", ownedInfo{uid: self}), qt.IsNil)
	c.Assert(checkPrivate("cache/entry.json", ownedInfo{uid: self + 1}), qt.ErrorMatches, `cache/entry\.json is owned by another user`)
}
