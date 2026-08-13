// Package sqlitevirtual refuses a schema comparison that would treat a live
// SQLite virtual table as an ordinary one.
//
// Every Ptah desired-state format -- Go annotations, HCL, YAML, native `.sql`
// schema files -- has no syntax for CREATE VIRTUAL TABLE, and the native SQL
// schema parser says so out loud: feeding it `ptah db read` output for a
// database holding one fails with `unsupported CREATE target: VIRTUAL`. A
// virtual table therefore cannot appear on the desired side of any comparison,
// which leaves the comparator two ways to be wrong and no way to be right:
//
//   - the desired state does not name it, and the comparator reads that silence
//     as intent and plans `DROP TABLE "docs"`. Measured on a live database:
//     `ptah schema apply --auto-approve` against a desired state naming only
//     the other table deleted an FTS5 index and its three rows, leaving
//     `no such table: docs`. `PRAGMA table_list` went from seven rows to one.
//     The desired state could not have asked for the table to be kept, so this
//     is deletion the operator has no way to decline;
//   - the desired state names it, which it can only do as an ordinary table.
//     The two objects are then treated as the same object and every authored
//     column difference is compared against a column list the module owns.
//     `ALTER TABLE ... ADD COLUMN` is not something SQLite accepts on a virtual
//     table, so the plan is unrunnable, and reporting no difference at all --
//     which is what suppressing the comparison alone does -- leaves an
//     incompatible object in place while claiming the schema is synced.
//
// Neither is a difference a plan can express, so both are refused here, before
// anything is compared, and named. See stokaro/ptah#1028.
//
// The refusal removes a capability, and AGENTS.md ("Compatibility never removes
// a capability. Constitute it, do not discard it.") does not allow that to be
// the end of the story. The two directions get different escapes because they
// are different requests:
//
//   - to keep the table, exclude it from the comparison. `--exclude docs`
//     already does this on every verb that compares, measured as
//     `Schema is synced, no changes to be made.` with the catalog untouched;
//   - to drop it, [AllowDropEnvVar] plans the drop exactly as before. It is an
//     environment variable rather than a flag for the reason
//     [go.5x5.cz/ptah/internal/reservedrole.AllowEnvVar] gives: the conformance
//     cli-surface tier asserts that ptah-compat registers exactly the flags the
//     pinned Atlas community binary registers.
//
// The opt-in covers only the removal. A desired ordinary table colliding with a
// live virtual one stays refused however it is set, because no value of an
// environment variable makes the planner able to turn one into the other.
package sqlitevirtual

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/envbool"
)

// AllowDropEnvVar plans the removal of a live virtual table the desired state
// does not declare, restoring what Ptah did before the refusal existed.
//
// Setting it never makes anything succeed that the engine would refuse: the
// statement planned is the `DROP TABLE` SQLite has always accepted for a
// virtual table, which also destroys the module's shadow tables and the index
// contents. It only decides whether Ptah is willing to plan it unasked.
const AllowDropEnvVar = "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP"

var allowDrop = envbool.New(AllowDropEnvVar, false)

// DropAllowed reports whether the opt-in lifts the removal refusal.
//
// Unset keeps the refusal and a valid false spelling keeps it too; an empty or
// unparsable value is a configuration error rather than a silent refusal.
func DropAllowed() (bool, error) {
	return allowDrop.Resolve()
}

// Table is one live virtual table and the module that owns it.
type Table struct {
	Schema string
	Name   string
	Module string
}

func (t Table) String() string {
	if t.Schema == "" {
		return fmt.Sprintf("%q (module %s)", t.Name, t.Module)
	}
	return fmt.Sprintf("%q.%q (module %s)", t.Schema, t.Name, t.Module)
}

// ValidateComparison refuses a comparison whose database side holds a virtual
// table, naming every offending table, its module, and the way out.
//
// It is called at the seams that already return an error and that every verb
// which compares a live database goes through. A comparison the desired state
// cannot express is refused there rather than planned badly here.
func ValidateComparison(dialect string, desired *goschema.Database, database *types.DBSchema) error {
	if platform.NormalizeDialect(dialect) != platform.SQLite {
		// A non-SQLite target has no virtual tables to classify, so this
		// subsystem is not invoked on that run and must not fail a MySQL plan
		// for a malformed value of its variable. Same boundary as
		// stokaro/ptah#1334: validate on every invocation of the subsystem that
		// owns the variable, and on no others.
		return nil
	}
	virtual := Tables(database)
	if len(virtual) == 0 {
		return nil
	}
	// Resolved before the tables are split, so a run holding any virtual table
	// refuses a malformed value instead of passing in silence.
	dropAllowed, err := DropAllowed()
	if err != nil {
		return err
	}

	declared := declaredTableNames(desired)
	var collisions, removals []Table
	for _, table := range virtual {
		if declared[identity(table.Schema, table.Name)] {
			collisions = append(collisions, table)
			continue
		}
		removals = append(removals, table)
	}

	if len(collisions) > 0 {
		return fmt.Errorf(
			"%w: the desired schema declares %s as an ordinary %s, and the database has %s as a virtual table: %s;"+
				" Ptah cannot convert one kind into the other, and comparing their columns would plan"+
				" ALTER TABLE statements SQLite refuses on a virtual table;"+
				" rename the declared table, or exclude the name from the comparison",
			ptaherr.ErrUnsupportedFeature,
			quotedNames(collisions),
			noun(len(collisions)),
			pronoun(len(collisions)),
			names(collisions),
		)
	}
	if dropAllowed || len(removals) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%w: the database has virtual %s %s that the desired schema does not declare;"+
			" no Ptah desired-state format can declare a virtual table, so the absence is not a request to drop"+
			" %s, and planning the removal would delete the index and everything in it;"+
			" exclude %s from the comparison to leave %s in place, or set %s=1 to plan the drop anyway",
		ptaherr.ErrUnsupportedFeature,
		noun(len(removals)),
		names(removals),
		pronoun(len(removals)),
		quotedNames(removals),
		pronoun(len(removals)),
		AllowDropEnvVar,
	)
}

// Tables lists the virtual tables a database schema holds, in a stable order.
func Tables(database *types.DBSchema) []Table {
	if database == nil {
		return nil
	}
	var virtual []Table
	for _, table := range database.Tables {
		if table.VirtualModule == "" {
			continue
		}
		virtual = append(virtual, Table{
			Schema: table.Schema,
			Name:   table.Name,
			Module: table.VirtualModule,
		})
	}
	sort.Slice(virtual, func(i, j int) bool {
		if virtual[i].Schema != virtual[j].Schema {
			return virtual[i].Schema < virtual[j].Schema
		}
		return virtual[i].Name < virtual[j].Name
	})
	return virtual
}

// declaredTableNames indexes the desired state by the identity the comparator
// matches on.
func declaredTableNames(desired *goschema.Database) map[string]bool {
	declared := make(map[string]bool)
	if desired == nil {
		return declared
	}
	for _, table := range desired.Tables {
		declared[identity(table.Schema, table.Name)] = true
	}
	return declared
}

// identity folds case because SQLite matches table names case-insensitively:
// `CREATE TABLE DOCS` collides with a virtual `docs`, and a comparison that
// missed that would refuse nothing and plan the ALTER anyway.
func identity(schema, name string) string {
	return strings.ToLower(schema) + "\x00" + strings.ToLower(name)
}

func names(tables []Table) string {
	rendered := make([]string, 0, len(tables))
	for _, table := range tables {
		rendered = append(rendered, table.String())
	}
	return strings.Join(rendered, ", ")
}

func quotedNames(tables []Table) string {
	rendered := make([]string, 0, len(tables))
	for _, table := range tables {
		rendered = append(rendered, fmt.Sprintf("%q", table.Name))
	}
	return strings.Join(rendered, ", ")
}

func noun(count int) string {
	if count == 1 {
		return "table"
	}
	return "tables"
}

func pronoun(count int) string {
	if count == 1 {
		return "it"
	}
	return "them"
}
