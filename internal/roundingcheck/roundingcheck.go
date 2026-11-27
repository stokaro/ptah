// Package roundingcheck writes what a generated migration file carries before
// a column type change that keeps fewer digits after the point: a decimal
// given a lower scale or put under an integer, or a temporal column given a
// lower fractional-second precision, which most servers convert without an
// error, rounding or cutting each value.
//
// The rows that matter are the ones the database holds when the file runs,
// which may be a different database at a later time than the one the file was
// generated against. So each such column gets a `-- +ptah check` of kind
// [migrator.CheckKindRoundedValues], whose assertion is the condition
// [roundingscan.Refuse] counts ([roundingscan.Assertion]). The migrator
// evaluates it before the body, on the database the file runs on, and refuses
// the migration while a row would change. A file generated with the rounding
// allowed carries a note instead, saying that no check reads the rows.
// Time-zone removal uses [migrator.CheckKindTimeZoneLoss] and refuses every
// non-NULL value, even when no fractional digit is lost.
package roundingcheck

import (
	"fmt"
	"strings"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/roundingscan"
	"ptah.run/internal/tableref"
	"ptah.run/migration/migrator"
)

// Checked is the text a migration file for dialect carries before its
// statements for changes: for each change, a comment naming the column and
// both types, and a check of kind [migrator.CheckKindRoundedValues] or
// [migrator.CheckKindTimeZoneLoss] named after the column. It holds while no
// row loses fractional digits or zone information. Each line ends with a line
// break. No change writes nothing. It refuses a change [roundingscan.Assertion]
// refuses.
func Checked(dialect string, changes []roundingscan.Change) (string, error) {
	var block strings.Builder
	for _, change := range changes {
		assertion, err := roundingscan.Assertion(dialect, change)
		if err != nil {
			return "", err
		}
		kind := migrator.CheckKindRoundedValues
		loss := "keeps fewer digits after the point"
		refused := "a row holds a value the server would round or cut"
		if change.DropsTimeZone() {
			kind = migrator.CheckKindTimeZoneLoss
			loss = "removes a time zone or offset"
			refused = "the column holds a non-NULL value"
		}
		directive, err := migrator.Check{
			Kind:   kind,
			Name:   Name(change),
			Assert: assertion,
		}.Directive()
		if err != nil {
			return "", fmt.Errorf("write the check of %s: %w", column(change), err)
		}
		writeLine(&block, fmt.Sprintf("%s goes from %s to %s, which %s; the check refuses the migration while %s",
			column(change), change.HeldType, change.DeclaredType, loss, refused))
		block.WriteString(directive)
		block.WriteByte('\n')
	}
	return block.String(), nil
}

// Allowed is the text a migration file carries before its statements for
// changes the plan was asked to make without reading the rows: for each
// change, the comment [Checked] writes for it with a note that no check reads
// the rows in place of the check. No change writes nothing.
func Allowed(changes []roundingscan.Change) string {
	var block strings.Builder
	for _, change := range changes {
		if change.DropsTimeZone() {
			writeLine(&block, fmt.Sprintf("%s goes from %s to %s, which removes a time zone or offset; "+
				"the information loss was allowed when this file was generated, so no check reads the rows",
				column(change), change.HeldType, change.DeclaredType))
			continue
		}
		writeLine(&block, fmt.Sprintf("%s goes from %s to %s, which keeps fewer digits after the point; "+
			"the rounding was allowed when this file was generated, so no check reads the rows and the server "+
			"rounds or cuts each value to the new digits",
			column(change), change.HeldType, change.DeclaredType))
	}
	return block.String()
}

// Name is the name of the check of change: the column, qualified by its
// table, as [tableref.Column] writes it. A refusal names the check by it.
func Name(change roundingscan.Change) string {
	ref, parsed := tableref.Parse(change.Table)
	if !parsed {
		return tableref.Column("", change.Table, change.Column)
	}
	return tableref.Column(ref.Schema, ref.Name, change.Column)
}

// writeLine writes text as one comment line, with any directive marker in it
// neutralized, so a name cannot make the comment an instruction.
func writeLine(block *strings.Builder, text string) {
	block.WriteString(sqlutil.LineComment(text))
	block.WriteByte('\n')
}

// column names the column of change in a comment: `column "price" of table
// "orders"`.
func column(change roundingscan.Change) string {
	return fmt.Sprintf("column %q of %s", change.Column, tableref.Phrase(change.Table))
}
