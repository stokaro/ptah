package testutils

// URLDatabasePath returns the path a rendered database URL names, from
// whichever field carries it.
//
// atlasurl.Parse carries a Windows drive path as the URL's Opaque and leaves
// Path empty, because url.URL.String() would escape the separator into %5C and
// no parser reads that back. On Unix the same address lands in Path.
//
// It is the same database either way, so a test about *which* database should
// not have to know which field this platform used.
func URLDatabasePath(opaque, path string) string {
	if opaque != "" {
		return opaque
	}
	return path
}
