package compare

import (
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/tableref"
)

// Coverage pairs the limits the two sides of one comparison declared about
// themselves, and answers the only question a comparator has to ask of them:
// is this difference a difference, or is it something one side never looked at?
//
// The two sides gate opposite directions, and that asymmetry is the whole
// point (stokaro/ptah#1276):
//
//   - the DESIRED state's limits gate REMOVALS. An object the database has and
//     a description does not name is a removal only when that description
//     claimed to describe such objects. `ptah-compat schema inspect` omits the
//     block types the pinned Atlas community binary v1.3.0 refuses to read;
//     applying that document back to the database it came from used to plan
//     `DROP EXTENSION`, because a presentation decision had become deletion
//     intent.
//   - the CURRENT state's limits gate ADDITIONS. An object a description names
//     and a read did not report is a creation only when that read looked. A
//     role reader scoped to the inspected schema, or a table read scoped to one
//     schema, reports nothing about what it did not read, and planning
//     `CREATE ROLE` for a role that exists fails the migration.
//
// The zero Coverage plans everything, so a comparison between two descriptions
// that declared no limits behaves exactly as it did before this type existed.
type Coverage struct {
	// Desired is what the desired-state description does not describe.
	Desired coverage.Set
	// Current is what the read of the live database did not look at.
	Current coverage.Set
}

// CoverageOf reads the limits both sides declared. Either side may be nil,
// which is a caller with nothing to compare rather than a caller claiming
// nothing is described.
func CoverageOf(generated *goschema.Database, database *types.DBSchema) Coverage {
	var cov Coverage
	if generated != nil {
		cov.Desired = generated.NotDescribed
	}
	if database != nil {
		cov.Current = database.NotDescribed
	}
	return cov
}

// PlansRemoval reports whether an object present in the database and absent
// from the desired state is a removal. Pass every spelling of the object's name
// the two sides might carry; the qualified and unqualified forms of one object
// are routinely spelled differently across the boundary, and a missed spelling
// restores the defect.
func (c Coverage) PlansRemoval(kind coverage.Kind, schema string, names ...string) bool {
	return c.Desired.DescribesIn(kind, schema, names...)
}

// PlansAddition reports whether an object named by the desired state and absent
// from the read is a creation.
func (c Coverage) PlansAddition(kind coverage.Kind, schema string, names ...string) bool {
	return c.Current.DescribesIn(kind, schema, names...)
}

// keepPlannedAdditions drops from a list of planned additions every object the
// read never looked at. names maps a planned entry to every spelling of it the
// coverage record might use.
func (c Coverage) keepPlannedAdditions(
	kind coverage.Kind,
	planned []string,
	names func(string) (schema string, spellings []string),
) []string {
	return c.keep(planned, func(entry string) bool {
		schema, spellings := names(entry)
		return c.PlansAddition(kind, schema, spellings...)
	})
}

// keepPlannedRemovals drops from a list of planned removals every object the
// desired state never claimed to describe.
func (c Coverage) keepPlannedRemovals(
	kind coverage.Kind,
	planned []string,
	names func(string) (schema string, spellings []string),
) []string {
	return c.keep(planned, func(entry string) bool {
		schema, spellings := names(entry)
		return c.PlansRemoval(kind, schema, spellings...)
	})
}

// keep preserves the input's nil-versus-empty shape, because several
// comparators publish an empty non-nil slice as part of their contract and a
// filter that quietly nils it changes an answer it was not asked about.
func (c Coverage) keep(planned []string, keepEntry func(string) bool) []string {
	if planned == nil {
		return nil
	}
	out := make([]string, 0, len(planned))
	for _, entry := range planned {
		if keepEntry(entry) {
			out = append(out, entry)
		}
	}
	return out
}

// qualifiedName is the spelling helper for objects a comparator keys by a
// possibly-qualified name. Both the whole name and its trailing part are
// offered as spellings, and the leading part is reported as the owning schema,
// so an object in a schema nobody read is recognized through the schema record
// even when nothing named the object itself.
//
// Splitting goes through [tableref.Parse] rather than a bare cut on the first
// dot, because a canonical reference quotes a component containing one and a
// naive split would take half an identifier for a schema.
func qualifiedName(name string) (schema string, spellings []string) {
	ref, ok := tableref.Parse(name)
	if !ok || !ref.Qualified {
		return "", []string{name}
	}
	return ref.Schema, []string{name, ref.Name}
}

// tableSchemaOnly reports only the schema a table belongs to. A table is
// covered through its schema rather than by its own name: the schema record is
// what a reader that skipped a whole namespace can produce, and the table names
// inside it are exactly what such a reader does not know.
func tableSchemaOnly(name string) (string, []string) {
	schema, _ := qualifiedName(name)
	return schema, nil
}

// globalName is the spelling helper for objects that have no owning schema:
// extensions are database-scoped and roles are cluster-scoped.
func globalName(name string) (string, []string) {
	return "", []string{name}
}
