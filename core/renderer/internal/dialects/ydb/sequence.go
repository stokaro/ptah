package ydb

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
)

// renderAlterSerialSequence writes the ALTER SEQUENCE that gives the sequence
// behind a Serial column its start and its increment.
//
// Both are written whatever changed, so the statement states the whole
// sequence rather than leaving a setting to whatever the server holds. RESTART
// is written only when the node asks for it, and then with the start spelled
// out: measured on 25.1.4.7 and 26.2.1.14, START alone changes the recorded
// start and leaves the next value where it was, and a restart onto a value a
// row holds fails the next insert with `Conflict with existing key`.
//
// YDB takes the sequence only by its absolute path (measured: a relative one
// answers `Path does not exist` whatever `PRAGMA TablePathPrefix` says), so a
// path without a leading slash is refused rather than sent.
func (r *Renderer) renderAlterSerialSequence(node *ast.AlterSerialSequenceNode) error {
	if node == nil {
		return fmt.Errorf("%w: %s: serial sequence node is nil", ptaherr.ErrInvalidSchemaDiff, DialectName)
	}
	subject := fmt.Sprintf("the sequence of Serial column %q of table %q", node.Column, node.Table)
	if !r.caps.Has(capability.SerialSequenceOptions) {
		return refuseKey(capability.SerialSequenceOptions, "changing "+subject)
	}
	if !strings.HasPrefix(node.Path, "/") {
		return refuseFact(subject, fmt.Sprintf("YDB's ALTER SEQUENCE takes a sequence only by its absolute path, "+
			"and %q is not one", node.Path))
	}
	statement := fmt.Sprintf("ALTER SEQUENCE %s START WITH %d INCREMENT BY %d", quote(node.Path), node.Start, node.Increment)
	if node.Restart {
		statement += fmt.Sprintf(" RESTART WITH %d", node.Start)
	}
	r.w.WriteLine(statement + ";")
	return nil
}
