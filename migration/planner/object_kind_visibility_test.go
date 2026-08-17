package planner_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
)

// everyPlannedDialect is every canonical dialect the planner registry routes,
// which is the set an object can be rendered for.
var everyPlannedDialect = []string{
	platform.Postgres,
	platform.CockroachDB,
	platform.YugabyteDB,
	platform.Spanner,
	platform.MySQL,
	platform.MariaDB,
	platform.ClickHouse,
	platform.SQLite,
	platform.SQLServer,
}

// visibilityAnswer classifies what one SQL text says about one object, without
// requiring any particular wording.
//
// The agreement matrix in object_capability_agreement_test.go asks a stricter
// question -- that both paths write ONE canonical skip comment -- and it asks it
// of the PostgreSQL family, whose four dialects share a renderer. That wording
// is not shared: the MySQL family writes "is not generated for this target",
// SQLite writes "is not supported", ClickHouse writes its own. Requiring one
// spelling across nine dialects would be a rename, not a fix.
//
// What stokaro/ptah#1628 is actually about survives the difference: the
// operator has to be able to SEE what happened to an object they declared. So
// the question here is whether the object's name appears at all.
func visibilityAnswer(sql, ddl, marker string) string {
	switch {
	case strings.Contains(sql, ddl):
		return "ddl"
	case strings.Contains(sql, marker):
		return "named"
	default:
		return "silent"
	}
}

// objectMarker is the distinctive name of the object a gate declares.
//
// gate.object is a human phrase for the failure report -- "on users to
// app_user" -- and no message contains it verbatim, so matching on it would
// score every refusal as silent. What a message has to carry for an operator to
// act on it is the name they wrote, which is this.
func objectMarker(gate objectKindGate) string {
	markers := map[string]string{
		"view":               "active_users",
		"materialized view":  "user_counts",
		"function":           "touch_updated",
		"trigger":            "users_touch",
		"sequence":           "order_number_seq",
		"role":               "app_user",
		"grant":              "app_user",
		"row-level security": "users",
		"policy":             "users_self",
	}
	return markers[gate.kind]
}

// TestObjectKinds_NoDialectDropsAnObjectWithoutNamingIt is the enumeration
// stokaro/ptah#1628 asks for: every object kind, every dialect, no silence.
//
// A target that cannot represent an object has two defensible answers, emit the
// DDL or name the omission. The third -- produce nothing and say nothing -- is
// the one this test exists to make impossible, because the operator's declared
// intent then disappears between the schema they wrote and the plan they
// review with nothing in the output saying so.
//
// Both paths are asked, because they are separate implementations and #929
// found them disagreeing: a renderer that names an omission does not make the
// planner do it.
func TestObjectKinds_NoDialectDropsAnObjectWithoutNamingIt(t *testing.T) {
	c := qt.New(t)

	type cell struct {
		dialect string
		gate    objectKindGate
		render  string
		plan    string
	}

	cells := make([]cell, 0, len(everyPlannedDialect)*len(objectKindGates))
	for _, dialect := range everyPlannedDialect {
		for _, gate := range objectKindGates {
			// One object kind per fixture. Sharing one fixture across the gates
			// confounds the measurement: the MySQL family answers a ROLE with a
			// whole-render error, which aborts before any other kind is
			// reached, and every one of them then reads as silent for a reason
			// that has nothing to do with that kind.
			//
			// Folded with the error text rather than asserted nil: a refusal is
			// a legitimate answer to an object a target cannot model -- "fails
			// closed", in the issue's words. What it must not be is silent, and
			// an error naming the object is not.
			database := singleObjectFixture(gate)
			cells = append(cells, cell{
				dialect: dialect,
				gate:    gate,
				render:  visibilityAnswer(renderedOrRefusal(database, dialect), gate.ddl, objectMarker(gate)),
				plan:    visibilityAnswer(plannedOrRefusal(database, dialect), gate.ddl, objectMarker(gate)),
			})
		}
	}

	// Control: a fixture or extractor that produced no cells would make the
	// assertion below pass while measuring nothing.
	c.Assert(cells, qt.HasLen, len(everyPlannedDialect)*len(objectKindGates))

	silent := slices.DeleteFunc(slices.Clone(cells), func(current cell) bool {
		return current.render != "silent" && current.plan != "silent"
	})
	lines := make([]string, 0, len(silent))
	for _, current := range silent {
		lines = append(lines, fmt.Sprintf("%-12s %-18s %-22s render=%s plan=%s",
			current.dialect, current.gate.kind, current.gate.object, current.render, current.plan))
	}
	c.Assert(lines, qt.HasLen, 0,
		qt.Commentf("%d of %d cells drop an object without naming it:\n%s",
			len(silent), len(cells), strings.Join(lines, "\n")))
}

// singleObjectFixture keeps the table every object attaches to and exactly one
// object kind, so a refusal of one kind cannot decide the answer for another.
//
// The role is kept alongside the grant and the two row-level-security kinds
// because those three name it: a grant to a role the schema never declares is a
// different fixture, not a smaller one.
func singleObjectFixture(gate objectKindGate) goschema.Database {
	full := objectKindFixture()
	kept := goschema.Database{
		Tables: full.Tables,
		// ONE column, where the shared fixture declares two.
		//
		// SQL Server compares column names through the catalog, because whether
		// they are case sensitive is a property of the database collation. With
		// no catalog to resolve against -- which is every offline plan -- each
		// name hashes to the same "unresolved" key, so any table with two
		// columns is refused for a possible identity collision before an object
		// kind is reached. That refusal is about the fixture, not about the
		// object under test, and it would make all nine SQL Server rows read as
		// silent for a reason that has nothing to do with them.
		Fields: full.Fields[:1],
	}
	switch gate.kind {
	case "view":
		kept.Views = full.Views
	case "materialized view":
		kept.MaterializedViews = full.MaterializedViews
	case "function":
		kept.Functions = full.Functions
	case "trigger":
		// Deliberately WITHOUT the function the trigger calls. A target that
		// refuses functions would otherwise refuse this fixture for the
		// function's sake and the trigger would never be reached, which is the
		// confounding this per-kind fixture exists to remove.
		kept.Triggers = full.Triggers
	case "sequence":
		kept.Sequences = full.Sequences
	case "role":
		kept.Roles = full.Roles
	case "grant":
		kept.Grants = full.Grants
	case "row-level security":
		kept.RLSEnabledTables = full.RLSEnabledTables
	case "policy":
		// Without the role either, for the same reason: the MySQL family and
		// ClickHouse refuse a declared role outright, so a fixture carrying one
		// measures the role rather than the policy. A policy naming a role the
		// schema does not declare is exactly what an RLS policy on an
		// externally managed role looks like.
		kept.RLSEnabledTables = full.RLSEnabledTables
		kept.RLSPolicies = full.RLSPolicies
	}
	return kept
}
