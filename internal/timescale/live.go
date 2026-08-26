// Package timescale holds the refusals and notes a live TimescaleDB server
// needs, for the parts of its model a declaration cannot carry.
package timescale

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
)

// ValidateLive refuses a declaration that names a relation the server holds as
// a continuous aggregate, and returns nil for every other dialect and every
// server without one.
//
// The refusal exists because the two names collide on a target where only one
// of them can win. A continuous aggregate occupies its name as a view --
// pg_class reports relkind 'v' -- so a declaration naming a view with that
// name reaches CREATE VIEW, and the server answers
// `relation "conditions_hourly" already exists` at apply time, halfway through
// a script. Refusing before anything is compared says which object it is and
// why, where the server's message says only that the name is taken.
//
// The declaration this refusal is most often meant for is an aggregate written
// as a materialized view, which is the natural mistake: PostgreSQL calls it one.
// The `continuous_aggregate` block and `//ptah:schema:continuousaggregate`
// declare the object itself, and neither reaches this refusal.
func ValidateLive(dialect string, generated *goschema.Database, database *catalog.Database) error {
	if !platform.IsPostgresFamily(dialect) {
		return nil
	}
	if generated == nil || database == nil || len(database.ContinuousAggregates) == 0 {
		return nil
	}

	live := make(map[string]catalog.ContinuousAggregate, len(database.ContinuousAggregates))
	for _, aggregate := range database.ContinuousAggregates {
		live[foldQualified(aggregate.Schema, aggregate.Name)] = aggregate
	}

	var problems []error
	for _, view := range generated.Views {
		problems = appendAggregateClash(problems, live, "view", view.Name)
	}
	for _, view := range generated.MaterializedViews {
		problems = appendAggregateClash(problems, live, "materialized view", view.Name)
	}
	for _, table := range generated.Tables {
		problems = appendAggregateClash(problems, live, "table", table.Name)
	}
	slices.SortFunc(problems, func(a, b error) int {
		return strings.Compare(a.Error(), b.Error())
	})
	return errors.Join(problems...)
}

// appendAggregateClash records a declared object whose name is already a
// continuous aggregate on the server.
func appendAggregateClash(
	problems []error,
	live map[string]catalog.ContinuousAggregate,
	kind, declared string,
) []error {
	schema, name := splitQualified(declared)
	aggregate, clashes := live[foldQualified(schema, name)]
	if !clashes {
		return problems
	}
	return append(problems, fmt.Errorf(
		"declared %s %q is a TimescaleDB continuous aggregate on this server, materializing "+
			"%s.%s: applying this declaration would create a relation the name already belongs "+
			"to; declare it as a continuous aggregate instead, rename the %s, or drop the "+
			"aggregate with DROP MATERIALIZED VIEW",
		kind, declared, aggregate.HypertableSchema, aggregate.HypertableName, kind))
}

// foldQualified builds the comparison key. An unqualified declaration and an
// aggregate the reader reported with a blank schema are the same object: the
// reader blanks the connection's own schema, and a declaration that names no
// schema means the same one.
func foldQualified(schema, name string) string {
	return strings.ToLower(strings.TrimSpace(schema)) + "\x00" +
		strings.ToLower(strings.TrimSpace(name))
}

// splitQualified separates an optional `schema.` prefix from a declared name.
func splitQualified(name string) (schema, bare string) {
	if before, after, found := strings.Cut(strings.TrimSpace(name), "."); found {
		return before, after
	}
	return "", strings.TrimSpace(name)
}
