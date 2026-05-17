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

	// The annotation omits security, volatility, AND language; PostgreSQL
	// reports the implicit defaults (INVOKER, VOLATILE, plpgsql for a typical
	// trigger/RLS helper). The comparator must normalize the Go side so no
	// spurious diff is reported.
	gen := goschema.Function{
		Name:    "f",
		Returns: "INTEGER",
		Body:    "BEGIN RETURN 1; END;",
	}
	db := dbtypes.DBFunction{
		Name:       "f",
		Returns:    "INTEGER",
		Language:   "plpgsql",
		Security:   "INVOKER",
		Volatility: "VOLATILE",
		Body:       "BEGIN RETURN 1; END;",
	}

	diff := compare.FunctionDefinitions(gen, db)
	c.Assert(diff.Changes, qt.HasLen, 0)
}

func TestFunctionDefinitions_LowercaseAnnotationDoesNotDiff(t *testing.T) {
	cases := []struct {
		name string
		gen  goschema.Function
	}{
		{
			name: "lowercase security/volatility",
			gen: goschema.Function{
				Name: "f", Returns: "VOID", Language: "plpgsql",
				Security: "definer", Volatility: "stable", Body: "BEGIN END;",
			},
		},
		{
			name: "mixed-case security/volatility",
			gen: goschema.Function{
				Name: "f", Returns: "VOID", Language: "plpgsql",
				Security: "Definer", Volatility: "Stable", Body: "BEGIN END;",
			},
		},
		{
			name: "uppercase language",
			gen: goschema.Function{
				Name: "f", Returns: "VOID", Language: "PLPGSQL",
				Security: "DEFINER", Volatility: "STABLE", Body: "BEGIN END;",
			},
		},
	}
	db := dbtypes.DBFunction{
		Name: "f", Returns: "VOID", Language: "plpgsql",
		Security: "DEFINER", Volatility: "STABLE", Body: "BEGIN END;",
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.FunctionDefinitions(tc.gen, db)
			c.Assert(diff.Changes, qt.HasLen, 0,
				qt.Commentf("normalization should produce a clean diff; got: %v", diff.Changes))
		})
	}
}

// TestFunctionDefinitions_ExplicitNonDefaultLanguage covers the pre-iteration-1
// case that was overwritten when the empty-language default was introduced: a
// function annotation that legitimately uses LANGUAGE sql (a pure-SQL helper,
// not plpgsql) must still produce a clean diff when both sides agree.
func TestFunctionDefinitions_ExplicitNonDefaultLanguage(t *testing.T) {
	c := qt.New(t)

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
