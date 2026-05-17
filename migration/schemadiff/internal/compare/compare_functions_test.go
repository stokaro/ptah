package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	dbtypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestFunctionDefinitions_DetectsBodyChange(t *testing.T) {
	c := qt.New(t)

	gen := goschema.Function{
		Name:       "set_tenant_context",
		Parameters: "tenant_id_param TEXT",
		Returns:    "VOID",
		Language:   "plpgsql",
		Security:   "DEFINER",
		Volatility: "VOLATILE",
		Body:       "BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, true); END;",
	}
	db := dbtypes.DBFunction{
		Name:       "set_tenant_context",
		Parameters: "tenant_id_param TEXT",
		Returns:    "VOID",
		Language:   "plpgsql",
		Security:   "DEFINER",
		Volatility: "VOLATILE",
		Body:       "BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;",
	}

	diff := compare.FunctionDefinitions(gen, db)

	c.Assert(diff.FunctionName, qt.Equals, "set_tenant_context")
	c.Assert(diff.Changes, qt.HasLen, 1)
	c.Assert(diff.Changes["body"], qt.Contains, "false")
	c.Assert(diff.Changes["body"], qt.Contains, "true")
}

func TestFunctionDefinitions_DetectsSecurityVolatilityLanguageChanges(t *testing.T) {
	c := qt.New(t)

	gen := goschema.Function{
		Name:       "f",
		Returns:    "INTEGER",
		Language:   "sql",
		Security:   "INVOKER",
		Volatility: "IMMUTABLE",
		Body:       "SELECT 1;",
	}
	db := dbtypes.DBFunction{
		Name:       "f",
		Returns:    "INTEGER",
		Language:   "plpgsql",
		Security:   "DEFINER",
		Volatility: "STABLE",
		Body:       "SELECT 1;",
	}

	diff := compare.FunctionDefinitions(gen, db)

	c.Assert(diff.Changes["language"], qt.Equals, "plpgsql -> sql")
	c.Assert(diff.Changes["security"], qt.Equals, "DEFINER -> INVOKER")
	c.Assert(diff.Changes["volatility"], qt.Equals, "STABLE -> IMMUTABLE")
	_, hasBody := diff.Changes["body"]
	c.Assert(hasBody, qt.IsFalse)
}

func TestFunctionDefinitions_NoChangeWhenIdentical(t *testing.T) {
	c := qt.New(t)

	fn := struct {
		gen goschema.Function
		db  dbtypes.DBFunction
	}{
		gen: goschema.Function{
			Name: "f", Returns: "VOID", Language: "plpgsql",
			Security: "INVOKER", Volatility: "VOLATILE", Body: "BEGIN END;",
		},
		db: dbtypes.DBFunction{
			Name: "f", Returns: "VOID", Language: "plpgsql",
			Security: "INVOKER", Volatility: "VOLATILE", Body: "BEGIN END;",
		},
	}

	diff := compare.FunctionDefinitions(fn.gen, fn.db)
	c.Assert(diff.Changes, qt.HasLen, 0)
}

func TestFunctionDefinitions_EmptyAnnotationDefaultsMatchPostgresDefaults(t *testing.T) {
	c := qt.New(t)

	// The annotation omits security and volatility; PostgreSQL reports the
	// implicit defaults (INVOKER, VOLATILE). The comparator must normalize
	// the Go side so no spurious diff is reported.
	gen := goschema.Function{
		Name:     "f",
		Returns:  "INTEGER",
		Language: "sql",
		Body:     "SELECT 1;",
	}
	db := dbtypes.DBFunction{
		Name:       "f",
		Returns:    "INTEGER",
		Language:   "sql",
		Security:   "INVOKER",
		Volatility: "VOLATILE",
		Body:       "SELECT 1;",
	}

	diff := compare.FunctionDefinitions(gen, db)
	c.Assert(diff.Changes, qt.HasLen, 0)
}

func TestFunctions_PopulatesModifiedList(t *testing.T) {
	c := qt.New(t)

	gen := &goschema.Database{
		Functions: []goschema.Function{
			{
				Name:       "f",
				Returns:    "VOID",
				Language:   "plpgsql",
				Security:   "INVOKER",
				Volatility: "VOLATILE",
				Body:       "BEGIN PERFORM 1; END;",
			},
		},
	}
	db := &dbtypes.DBSchema{
		Functions: []dbtypes.DBFunction{
			{
				Name:       "f",
				Returns:    "VOID",
				Language:   "plpgsql",
				Security:   "INVOKER",
				Volatility: "VOLATILE",
				Body:       "BEGIN PERFORM 2; END;",
			},
		},
	}

	diff := &difftypes.SchemaDiff{}
	compare.Functions(gen, db, diff)

	c.Assert(diff.FunctionsAdded, qt.HasLen, 0)
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 0)
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].FunctionName, qt.Equals, "f")
	c.Assert(diff.FunctionsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}
