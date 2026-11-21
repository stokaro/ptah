//go:build !unix

package advisorysource

import "io/fs"

// checkPrivate accepts every cache path. On Windows the default user cache
// directory, %LocalAppData%, is private to its user by the access control
// list Windows gives it, and Ptah does not read the list.
func checkPrivate(string, fs.FileInfo) error { return nil }
