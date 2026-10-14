//go:build integration

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// TestOracleHierarchyRootTypesAreNotDescribedLive pins what the read does with
// a NOT FINAL or NOT INSTANTIABLE object type: it records the type as one it
// cannot describe, and a declaration of that name is undecided.
//
// The model holds an attribute list and nothing else, so a NOT FINAL type was
// described as a FINAL one. A comparison then called a declaration of the same
// attributes the same type, and `schema inspect` wrote a statement that
// replays as FINAL, under which a subtype is created with PLS-00590, measured
// on 23.26.3.0.0.
func TestOracleHierarchyRootTypesAreNotDescribedLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	dropOracleHierarchyRoots(context.WithoutCancel(ctx), conn)
	defer dropOracleHierarchyRoots(context.WithoutCancel(ctx), conn)
	execOracle(ctx, c, conn, "CREATE OR REPLACE TYPE ohr_open AS OBJECT (a NUMBER) NOT FINAL")
	execOracle(ctx, c, conn, "CREATE OR REPLACE TYPE ohr_abstract AS OBJECT (a NUMBER) NOT INSTANTIABLE NOT FINAL")

	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleCompositeNames(c, live), qt.Not(qt.Contains), "OHR_OPEN")
	c.Assert(unrepresentableHierarchyRoots(live.FeatureCoverage), qt.DeepEquals, []string{
		"OHR_ABSTRACT: the object type is NOT INSTANTIABLE, which the object type model cannot describe",
		"OHR_OPEN: the object type is NOT FINAL, which the object type model cannot describe",
	})

	attributes := []oratype.Attribute{{Name: "a", Type: "NUMBER"}}
	declared := oracleTypesDeclared(c,
		oracleTypeObject(c, oratype.ObjectTypeKind, "ohr_open", &oratype.DesiredObjectType{Attributes: attributes}))
	_, err = schemadiff.CompareWithDatabase(ctx, conn, declared, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
	c.Assert(err, qt.ErrorMatches, `(?s).*the object type exists and the read cannot describe it: the object type is NOT FINAL.*`)
}

// unrepresentableHierarchyRoots lists the fixture's object types the read
// found and could not describe, as "<name>: <reason>".
func unrepresentableHierarchyRoots(coverage schemaext.Coverage) []string {
	var listed []string
	for _, record := range coverage.SubjectRecords() {
		if record.Kind == oratype.ObjectTypeKind && record.Knowledge.State == schemaext.Unrepresentable &&
			strings.HasPrefix(record.Subject.Name.Source, "OHR_") {
			listed = append(listed, record.Subject.Name.Source+": "+record.Knowledge.Reason)
		}
	}
	return listed
}

// dropOracleHierarchyRoots removes the fixture, tolerating what is absent.
func dropOracleHierarchyRoots(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, statement := range []string{"DROP TYPE ohr_open", "DROP TYPE ohr_abstract"} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, statement)
	}
}
