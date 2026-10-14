//go:build integration

package integration_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/oratype"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestOracleTypesTheReadCannotDescribeAreNotReplacedLive pins what a plan does
// with a declaration that names a type the schema holds and the read cannot
// describe: a domain of two columns, an enumeration or flexible domain, an
// object type with methods, a subtype, or a collection type.
//
// The read records each one as unrepresentable. When it left them out and
// still claimed to describe every type, a declaration of that name planned
// CREATE OR REPLACE TYPE over the object type with methods, which drops the
// methods, and CREATE DOMAIN over the domain of two columns. A flexible domain
// failed the whole read with a duplicate identity.
func TestOracleTypesTheReadCannotDescribeAreNotReplacedLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Oracle)
	c := qt.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	fixture := undescribedOracleTypes(conn.Info().Capabilities)
	dropUndescribedOracleTypes(context.WithoutCancel(ctx), conn)
	defer dropUndescribedOracleTypes(context.WithoutCancel(ctx), conn)
	for _, statement := range fixture.statements {
		execOracle(ctx, c, conn, statement)
	}

	live, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(unrepresentableOracleTypes(live.FeatureCoverage), qt.DeepEquals, fixture.unrepresentable)

	declared := oracleTypesDeclared(c, fixture.declared...)
	_, err = schemadiff.CompareWithDatabase(ctx, conn, declared, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
	var incomplete *schemadiff.IncompleteComparisonError
	c.Assert(err, qt.ErrorAs, &incomplete)
	c.Assert(undecidedOracleTypes(incomplete.Diagnostics), qt.DeepEquals, fixture.undecided)

	// A caller that takes the undecided changes as a report plans the rest,
	// and the rest replaces nothing.
	diff, undecided, err := schemadiff.CompareWithDatabaseReportingUndecidedAdditions(ctx, conn, declared, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(undecidedOracleTypes(undecided), qt.DeepEquals, fixture.undecided)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		context.Background(), must.Must(builtin.New()),
		diff, platform.Oracle, planner.Options{Capabilities: conn.Info().Capabilities},
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statementsCreatingOracleTypes(statements), qt.HasLen, 0)
}

// undescribedOracleTypeFixture is the types a test creates, the claims the
// read records about them, the declarations naming them and the undecided
// changes those declarations become.
type undescribedOracleTypeFixture struct {
	statements      []string
	unrepresentable []string
	declared        []schemaext.Object
	undecided       []string
}

// undescribedOracleTypes is the fixture for the target's release line. Oracle
// 21 has no domains, so it holds the object types alone.
func undescribedOracleTypes(caps capability.Capabilities) undescribedOracleTypeFixture {
	fixture := undescribedOracleTypeFixture{
		statements: []string{
			"CREATE OR REPLACE TYPE cov_method AS OBJECT (a NUMBER, MEMBER FUNCTION doubled RETURN NUMBER)",
			"CREATE OR REPLACE TYPE cov_parent AS OBJECT (a NUMBER) NOT FINAL",
			"CREATE OR REPLACE TYPE cov_child UNDER cov_parent (b NUMBER)",
			"CREATE OR REPLACE TYPE cov_list AS VARRAY(4) OF NUMBER",
		},
		unrepresentable: []string{
			"object-type COV_CHILD: the object type is a subtype of COV_PARENT, which the object type model cannot describe",
			"object-type COV_LIST: the type is a collection type, which the object type model cannot describe",
			"object-type COV_METHOD: the object type has methods, which the object type model cannot describe",
			"object-type COV_PARENT: the object type is NOT FINAL, which the object type model cannot describe",
		},
		declared: []schemaext.Object{
			must.Must(oratype.DesiredObject(oratype.Ref(oratype.ObjectTypeKind, "", "cov_method"),
				&oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "a", Type: "NUMBER"}}})),
			must.Must(oratype.DesiredObject(oratype.Ref(oratype.ObjectTypeKind, "", "cov_list"),
				&oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "a", Type: "NUMBER"}}})),
		},
		undecided: []string{
			"cov_list: the object type exists and the read cannot describe it: " +
				"the type is a collection type, which the object type model cannot describe",
			"cov_method: the object type exists and the read cannot describe it: " +
				"the object type has methods, which the object type model cannot describe",
		},
	}
	if !caps.Has(capability.DomainTypes) {
		return fixture
	}
	fixture.statements = append(fixture.statements,
		"CREATE DOMAIN cov_single AS NUMBER",
		"CREATE DOMAIN cov_multi AS (lat AS NUMBER, lon AS NUMBER)",
		"CREATE DOMAIN cov_enum AS ENUM (red, green)",
		"CREATE FLEXIBLE DOMAIN cov_flex (a) CHOOSE DOMAIN USING (k NUMBER) FROM (CASE k WHEN 1 THEN cov_single(a) END)",
	)
	fixture.unrepresentable = slices.Concat([]string{
		"domain COV_ENUM: the domain is an enumeration domain, which the domain model cannot describe",
		"domain COV_FLEX: the domain is a flexible domain, which the domain model cannot describe",
		"domain COV_MULTI: the domain has 2 columns, and the domain model describes a single-column domain",
	}, fixture.unrepresentable)
	fixture.declared = append(fixture.declared,
		must.Must(oratype.DesiredObject(oratype.Ref(oratype.DomainKind, "", "cov_multi"), &oratype.DesiredDomain{BaseType: "NUMBER"})),
		must.Must(oratype.DesiredObject(oratype.Ref(oratype.DomainKind, "", "cov_enum"), &oratype.DesiredDomain{BaseType: "NUMBER"})),
	)
	fixture.undecided = slices.Concat([]string{
		"cov_enum: the domain exists and the read cannot describe it: " +
			"the domain is an enumeration domain, which the domain model cannot describe",
		"cov_multi: the domain exists and the read cannot describe it: " +
			"the domain has 2 columns, and the domain model describes a single-column domain",
	}, fixture.undecided)
	slices.Sort(fixture.undecided)
	return fixture
}

// dropUndescribedOracleTypes removes what the test creates, dependents first,
// tolerating what is absent.
func dropUndescribedOracleTypes(ctx context.Context, conn *dbschema.DatabaseConnection) {
	for _, statement := range []string{
		"DROP DOMAIN cov_flex", "DROP DOMAIN cov_single", "DROP DOMAIN cov_multi", "DROP DOMAIN cov_enum",
		"DROP TYPE cov_child", "DROP TYPE cov_parent", "DROP TYPE cov_method", "DROP TYPE cov_list",
	} {
		_ = conn.SchemaWriter().ExecuteSQL(ctx, statement)
	}
}

// unrepresentableOracleTypes lists the fixture's Oracle types a read found and
// could not describe, as "<model> <name>: <reason>", in kind and name order.
// Types the fixture did not create are left out, since the schema is shared.
func unrepresentableOracleTypes(coverage schemaext.Coverage) []string {
	var listed []string
	for _, record := range coverage.SubjectRecords() {
		if slices.Contains(oratype.Kinds(), record.Kind) && record.Knowledge.State == schemaext.Unrepresentable &&
			strings.HasPrefix(record.Subject.Name.Source, "COV_") {
			model := strings.TrimPrefix(string(record.Kind), "ptah.run/oratype/")
			listed = append(listed, model+" "+record.Subject.Name.Source+": "+record.Knowledge.Reason)
		}
	}
	return listed
}

// undecidedOracleTypes lists the Oracle types a comparison left undecided, as
// "<declared name>: <reason>", sorted.
func undecidedOracleTypes(diagnostics schemadiff.Diagnostics) []string {
	var listed []string
	for _, diagnostic := range diagnostics.Features {
		if slices.Contains(oratype.Kinds(), diagnostic.Kind) {
			listed = append(listed, diagnostic.Subject.Name.Source+": "+diagnostic.Reason)
		}
	}
	slices.Sort(listed)
	return listed
}

// statementsCreatingOracleTypes lists the planned statements that create or
// replace a domain or a type.
func statementsCreatingOracleTypes(statements []string) []string {
	var found []string
	for _, statement := range statements {
		folded := strings.ToUpper(statement)
		if strings.Contains(folded, "CREATE DOMAIN") || strings.Contains(folded, "CREATE OR REPLACE TYPE") ||
			strings.Contains(folded, "CREATE TYPE") {
			found = append(found, statement)
		}
	}
	return found
}
