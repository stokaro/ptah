package migrationlintreport

import (
	"fmt"
	"io"
)

// WriteServerVersionNotice says what a run planned against when the version it
// was given selected no measured release line, and writes nothing otherwise.
//
// It is shared because both command surfaces owe the same sentence. A run that
// planned against a release line the operator did not name has to say so, and
// the compatibility surface reads the same `.ptah-lint.yaml` the native one
// does. What differs is where it goes, which each caller decides: prose must
// not land inside a document a consumer decodes.
//
// A clean run is exactly when this matters most -- a clean run against an
// unmodeled server is the case it exists to announce -- so it is not
// conditional on findings.
func WriteServerVersionNotice(w io.Writer, report Report) error {
	if report.ServerVersionNote == "" {
		return nil
	}
	_, err := fmt.Fprintf(w, "warning: %s\n", report.ServerVersionNote)
	return err
}
