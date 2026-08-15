package testutils

import "os"

// StatMissingText returns the sentence os.Stat produces for a path that is not
// there, on the operating system running the test.
//
// It exists so an expectation can be derived rather than written out. The
// wording is the platform's, not Ptah's:
//
//	unix     stat missing: no such file or directory
//	windows  GetFileAttributesEx missing: The system cannot find the file specified.
//
// A test that spells one of them out asserts the platform it happens to run on
// — and a test that drops the clause entirely stops checking that the failure
// was about that path at all. Deriving it keeps the assertion byte-exact and
// portable at the same time, the way the DML placeholder matrix derives its
// expectation from sqlutil.Rebind rather than restating it.
//
// A path that unexpectedly exists returns a sentence that cannot match any
// diagnostic, so the caller's comparison fails loudly rather than silently
// comparing empty strings.
func StatMissingText(path string) string {
	if _, err := os.Stat(path); err != nil {
		return err.Error()
	}
	return "<" + path + " unexpectedly exists>"
}
