package mysqlroutine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/mysqlroutine"
)

// TestCharacteristic_EncodesEachVolatilityOnADistinctCatalogCell pins the write
// half of the round trip.
//
// The three clauses must be distinguishable in the catalog, because the read
// half recovers the value from what the catalog reports and nothing else. When
// STABLE and VOLATILE shared one clause, a declared STABLE function reported
// `volatility: VOLATILE -> STABLE` after a successful apply and planned the
// same destructive drop and create forever -- measured on MySQL 26.7.0 and
// MariaDB 12.3.2, still there after a second apply.
func TestCharacteristic_EncodesEachVolatilityOnADistinctCatalogCell(t *testing.T) {
	tests := []struct {
		name       string
		volatility string
		want       string
	}{
		{name: "immutable is deterministic", volatility: "IMMUTABLE", want: "DETERMINISTIC"},
		{name: "stable takes the remaining cell", volatility: "STABLE", want: "NOT DETERMINISTIC NO SQL"},
		{name: "volatile keeps its shipped spelling", volatility: "VOLATILE", want: "READS SQL DATA"},
		{name: "unset is volatile", volatility: "", want: "READS SQL DATA"},
		{name: "lowercase is accepted", volatility: "stable", want: "NOT DETERMINISTIC NO SQL"},
		{name: "surrounding space is trimmed", volatility: "  IMMUTABLE  ", want: "DETERMINISTIC"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := mysqlroutine.Characteristic(test.volatility)
			c.Assert(err, qt.IsNil)
			c.Check(got, qt.Equals, test.want)
		})
	}

	t.Run("the three clauses are three distinct values", func(t *testing.T) {
		c := qt.New(t)
		seen := make(map[string]string)
		for _, volatility := range []string{
			mysqlroutine.Immutable, mysqlroutine.Stable, mysqlroutine.Volatile,
		} {
			clause, err := mysqlroutine.Characteristic(volatility)
			c.Assert(err, qt.IsNil)
			previous, collided := seen[clause]
			c.Check(collided, qt.IsFalse,
				qt.Commentf("%s and %s both render %q, so a read cannot tell them apart",
					previous, volatility, clause))
			seen[clause] = volatility
		}
	})
}

// TestCharacteristic_RefusesAValueItCannotEncode holds the refusal.
//
// There is no fourth cell. Rendering an unknown volatility as one of the three
// would silently reinterpret it, which is the failure mode this package exists
// to end.
func TestCharacteristic_RefusesAValueItCannotEncode(t *testing.T) {
	tests := []struct {
		name       string
		volatility string
	}{
		{name: "misspelled", volatility: "STABEL"},
		{name: "postgres-only spelling", volatility: "LEAKPROOF"},
		{name: "nonsense", volatility: "sometimes"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := mysqlroutine.Characteristic(test.volatility)
			c.Assert(err, qt.IsNotNil)
			c.Check(err.Error(), qt.Contains, test.volatility)
		})
	}
}

// TestVolatilityFromCatalog_InvertsCharacteristic is the round trip stated as a
// property: every value Characteristic can write comes back out.
//
// The catalog columns are the ones measured on both engines for each clause.
// A mutant that reads only IS_DETERMINISTIC -- which is what the reader did --
// fails the STABLE row, because it is NOT DETERMINISTIC exactly like VOLATILE.
func TestVolatilityFromCatalog_InvertsCharacteristic(t *testing.T) {
	tests := []struct {
		name            string
		volatility      string
		isDeterministic string
		sqlDataAccess   string
	}{
		{
			name: "immutable", volatility: mysqlroutine.Immutable,
			isDeterministic: "YES", sqlDataAccess: "CONTAINS SQL",
		},
		{
			name: "stable", volatility: mysqlroutine.Stable,
			isDeterministic: "NO", sqlDataAccess: "NO SQL",
		},
		{
			name: "volatile", volatility: mysqlroutine.Volatile,
			isDeterministic: "NO", sqlDataAccess: "READS SQL DATA",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Check(
				mysqlroutine.VolatilityFromCatalog(test.isDeterministic, test.sqlDataAccess),
				qt.Equals, test.volatility)
		})
	}

	t.Run("a routine outside the encoding still reads as volatile", func(t *testing.T) {
		c := qt.New(t)
		c.Check(mysqlroutine.VolatilityFromCatalog("NO", "MODIFIES SQL DATA"),
			qt.Equals, mysqlroutine.Volatile)
	})
}

// TestSecurityClause_RefusesAnUnknownMode is the security half of the same rule.
//
// The old default branch rendered nothing for an unrecognized value, so MySQL
// applied DEFINER -- the broader right -- and the next comparison reported
// `security: DEFINER -> INVKOER` on a successful apply, forever. Measured on
// MySQL 26.7.0 and MariaDB 12.3.2.
func TestSecurityClause_RefusesAnUnknownMode(t *testing.T) {
	accepted := []struct {
		name     string
		security string
		want     string
	}{
		{name: "definer", security: "DEFINER", want: "SQL SECURITY DEFINER"},
		{name: "invoker", security: "INVOKER", want: "SQL SECURITY INVOKER"},
		{name: "lowercase invoker", security: "invoker", want: "SQL SECURITY INVOKER"},
		{name: "unset renders no clause", security: "", want: ""},
	}

	for _, test := range accepted {
		t.Run("accepted/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := mysqlroutine.SecurityClause(test.security)
			c.Assert(err, qt.IsNil)
			c.Check(got, qt.Equals, test.want)
		})
	}

	refused := []struct {
		name     string
		security string
	}{
		{name: "transposed letters", security: "INVKOER"},
		{name: "postgres spelling", security: "SECURITY DEFINER"},
		{name: "nonsense", security: "everyone"},
	}

	for _, test := range refused {
		t.Run("refused/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := mysqlroutine.SecurityClause(test.security)
			c.Assert(err, qt.IsNotNil)
			c.Check(err.Error(), qt.Contains, test.security)
		})
	}
}

// TestNormalizeType_ResolvesAliasesAndStripsTheDisplayWidth pins both halves of
// the type rule, which have different causes.
//
// The alias rows are the DESIRED side learning what the catalog will say: every
// pair was measured by declaring `RETURNS <spelling>` on MySQL 26.7.0 and
// MariaDB 12.3.2 and reading DTD_IDENTIFIER back, and both engines agreed on
// the base type for every one.
//
// The width rows are the two ENGINES disagreeing with each other: the same
// declaration reports `int` on MySQL and `int(11)` on MariaDB.
//
// The rows that must NOT change are the control. A mutant that truncated at the
// first "(" would pass every integer row and fail varchar, decimal and enum.
func TestNormalizeType_ResolvesAliasesAndStripsTheDisplayWidth(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// Measured aliases: declared spelling -> catalog base type.
		{name: "integer is int", in: "integer", want: "int"},
		{name: "int4 is int", in: "int4", want: "int"},
		{name: "int1 is tinyint", in: "int1", want: "tinyint"},
		{name: "int2 is smallint", in: "int2", want: "smallint"},
		{name: "int3 is mediumint", in: "int3", want: "mediumint"},
		{name: "middleint is mediumint", in: "middleint", want: "mediumint"},
		{name: "int8 is bigint", in: "int8", want: "bigint"},
		{name: "numeric is decimal", in: "numeric(10,2)", want: "decimal(10,2)"},
		{name: "dec is decimal", in: "dec(10,2)", want: "decimal(10,2)"},
		{name: "fixed is decimal", in: "fixed(10,2)", want: "decimal(10,2)"},
		{name: "bool is tinyint", in: "bool", want: "tinyint"},
		{name: "boolean is tinyint", in: "boolean", want: "tinyint"},
		{name: "double precision is double", in: "double precision", want: "double"},
		// REAL and the NATIONAL spellings are deliberately NOT folded; they are
		// refused instead, so they pass through here unchanged. See
		// TestValidateSignature_RefusesTypesThatCannotRoundTrip.
		{name: "real is left alone", in: "real", want: "real"},
		{name: "national varchar is left alone", in: "national varchar(10)", want: "national varchar(10)"},
		{name: "character varying is varchar", in: "character varying(20)", want: "varchar(20)"},
		{name: "character is char", in: "character(5)", want: "char(5)"},
		{name: "uppercase alias is resolved", in: "INTEGER", want: "int"},

		// The two engines disagreeing about display width.
		{name: "mariadb int width", in: "int(11)", want: "int"},
		{name: "mysql int already bare", in: "int", want: "int"},
		{name: "bigint width", in: "bigint(20)", want: "bigint"},
		{name: "tinyint width", in: "tinyint(1)", want: "tinyint"},
		{name: "smallint width", in: "smallint(6)", want: "smallint"},
		{name: "mediumint width", in: "mediumint(9)", want: "mediumint"},
		{name: "unsigned suffix survives", in: "int(10) unsigned", want: "int unsigned"},

		// Control: parentheses that carry meaning are untouched.
		{name: "varchar length is meaning, not width", in: "varchar(20)", want: "varchar(20)"},
		{name: "decimal precision is meaning", in: "decimal(10,2)", want: "decimal(10,2)"},
		{name: "text has no parentheses", in: "text", want: "text"},
		{name: "enum members are meaning", in: "enum('a','b')", want: "enum('a','b')"},
		{name: "surrounding space is trimmed", in: "  int(11)  ", want: "int"},
		{name: "empty stays empty", in: "", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Check(mysqlroutine.NormalizeType(test.in), qt.Equals, test.want)
		})
	}
}

// TestValidateSignature_RefusesTypesThatCannotRoundTrip pins the two families
// whose catalog form the declaration alone does not decide.
//
// Both were measured on MySQL 26.7.0. REAL depends on the connection's SQL
// mode: `RETURNS REAL` reports DTD_IDENTIFIER `double` under the image's
// default sql_mode and `float` with REAL_AS_FLOAT added to the same session,
// while DOUBLE, DOUBLE PRECISION and FLOAT report `double`, `double` and
// `float` under BOTH -- so the unambiguous spellings stay accepted and are the
// control here. The NATIONAL family reports the SAME DTD_IDENTIFIER as the
// plain spelling and differs only in CHARACTER_SET_NAME (`utf8mb3` against
// `utf8mb4`), a column this comparison does not read, so folding them made a
// real character-set change invisible.
//
// They are refused rather than merely left out of the synonym table, and that
// distinction is the whole point: leaving them unfolded would keep the declared
// spelling on the desired side against a different catalog spelling, which is
// permanent drift -- the failure this package exists to end.
func TestValidateSignature_RefusesTypesThatCannotRoundTrip(t *testing.T) {
	refused := []struct {
		name       string
		parameters string
		returns    string
		want       string
	}{
		{name: "real return", returns: "REAL", want: "REAL_AS_FLOAT"},
		{name: "real parameter", parameters: "a REAL", returns: "int", want: "REAL_AS_FLOAT"},
		{name: "national varchar return", returns: "NATIONAL VARCHAR(10)", want: "character set"},
		{name: "national char parameter", parameters: "a NATIONAL CHAR(5)", returns: "int", want: "character set"},
		{name: "nvarchar return", returns: "NVARCHAR(10)", want: "character set"},
		{name: "nchar parameter", parameters: "a NCHAR(5)", returns: "int", want: "character set"},
		{name: "national varchar without a length", returns: "NATIONAL VARCHAR", want: "character set"},
	}

	for _, test := range refused {
		t.Run("refused/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			err := mysqlroutine.ValidateSignature(test.parameters, test.returns)
			c.Assert(err, qt.IsNotNil)
			c.Check(err.Error(), qt.Contains, test.want)
		})
	}

	accepted := []struct {
		name       string
		parameters string
		returns    string
	}{
		{name: "double is unambiguous", returns: "DOUBLE"},
		{name: "double precision is unambiguous", returns: "DOUBLE PRECISION"},
		{name: "float is unambiguous", returns: "FLOAT"},
		{name: "plain varchar", parameters: "a VARCHAR(10)", returns: "VARCHAR(10)"},
		{name: "plain char", parameters: "a CHAR(5)", returns: "int"},
		{name: "integer alias is still folded, not refused", parameters: "a INTEGER", returns: "INTEGER"},
		{name: "empty signature", parameters: "", returns: "int"},
	}

	for _, test := range accepted {
		t.Run("accepted/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Check(mysqlroutine.ValidateSignature(test.parameters, test.returns), qt.IsNil)
		})
	}
}

// TestNormalizeType_KeepsTheWidthUnderZEROFILL pins the one case where an
// integer's parenthesized argument is meaning rather than display width.
//
// Measured on MySQL 26.7.0 AND MariaDB 12.3.2, which agree exactly here --
// unlike the plain integer case that motivated the width stripping:
//
//	INT(5) ZEROFILL           -> int(5) unsigned zerofill
//	INT(10) ZEROFILL          -> int(10) unsigned zerofill
//	INT(5) UNSIGNED ZEROFILL  -> int(5) unsigned zerofill
//
// Dropping the width collapsed the first two onto `int zerofill`, so changing
// the padding width produced no modification and was never applied. The
// `unsigned` is added because ZEROFILL implies it and both catalogs write the
// implication out; without that the desired side never matched the catalog even
// with the width kept.
//
// The plain-integer rows are the control: they must still lose their width, or
// the two engines stop agreeing with each other.
func TestNormalizeType_KeepsTheWidthUnderZEROFILL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "width is meaning under zerofill", in: "int(5) zerofill", want: "int(5) unsigned zerofill"},
		{name: "a different width is a different type", in: "int(10) zerofill", want: "int(10) unsigned zerofill"},
		{name: "catalog spelling is a fixed point", in: "int(5) unsigned zerofill", want: "int(5) unsigned zerofill"},
		{name: "uppercase declaration", in: "INT(5) ZEROFILL", want: "int(5) unsigned zerofill"},
		{name: "bigint zerofill", in: "bigint(20) zerofill", want: "bigint(20) unsigned zerofill"},
		// Control: without zerofill the width is a display width and goes.
		{name: "plain int keeps losing its width", in: "int(11)", want: "int"},
		{name: "unsigned without zerofill still loses the width", in: "int(10) unsigned", want: "int unsigned"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Check(mysqlroutine.NormalizeType(test.in), qt.Equals, test.want)
		})
	}

	t.Run("two widths do not collapse onto one type", func(t *testing.T) {
		c := qt.New(t)
		c.Check(mysqlroutine.NormalizeType("int(5) zerofill"),
			qt.Not(qt.Equals), mysqlroutine.NormalizeType("int(10) zerofill"))
	})
}

// TestValidateSignature_RefusesZEROFILLWithoutAWidth holds the edge the width
// rule cannot close.
//
// Measured on both engines, `INT ZEROFILL` is reported as
// `int(10) unsigned zerofill`: the server substitutes its own default rather
// than recording "unspecified", and the desired side cannot predict it without
// a per-type table of engine defaults. Written WITH a width it round-trips
// exactly, which is why the refusal asks for the width rather than rejecting
// ZEROFILL outright -- and the accepted rows below are that control.
func TestValidateSignature_RefusesZEROFILLWithoutAWidth(t *testing.T) {
	refused := []struct {
		name       string
		parameters string
		returns    string
	}{
		{name: "return type", returns: "INT ZEROFILL"},
		{name: "parameter", parameters: "a INT ZEROFILL", returns: "int"},
		{name: "bigint", returns: "BIGINT ZEROFILL"},
	}

	for _, test := range refused {
		t.Run("refused/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			err := mysqlroutine.ValidateSignature(test.parameters, test.returns)
			c.Assert(err, qt.IsNotNil)
			c.Check(err.Error(), qt.Contains, "ZEROFILL")
			c.Check(err.Error(), qt.Contains, "width")
		})
	}

	accepted := []struct {
		name       string
		parameters string
		returns    string
	}{
		{name: "width makes it decidable", returns: "INT(10) ZEROFILL"},
		{name: "width on a parameter", parameters: "a INT(5) ZEROFILL", returns: "int"},
		{name: "no zerofill at all", returns: "INT"},
	}

	for _, test := range accepted {
		t.Run("accepted/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Check(mysqlroutine.ValidateSignature(test.parameters, test.returns), qt.IsNil)
		})
	}
}

// TestNormalizeParameterList_SplitsAtTopLevelCommasOnly pins that a comma
// belonging to a value is not a parameter separator.
//
// `ENUM('x,y')` and `ENUM('x','y')` are different member sets and both catalogs
// report them as such. Splitting on every comma turned the first into two
// parameters and reassembled it as `enum('x, y')`, so the two normalized
// identically and changing one to the other produced no modification at all --
// a real signature change that could never be applied.
//
// The `DECIMAL(10,2)` row is the same trap in the shape that is easy to reach
// by accident, and the two-parameter rows are the control: real separators must
// still separate.
func TestNormalizeParameterList_SplitsAtTopLevelCommasOnly(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "comma inside an enum member", in: "p ENUM('x,y')", want: "p enum('x,y')"},
		{name: "separate enum members", in: "p ENUM('x','y')", want: "p enum('x','y')"},
		{name: "comma inside decimal precision", in: "d DECIMAL(10,2)", want: "d decimal(10,2)"},
		{
			name: "real separators still separate",
			in:   "a INTEGER, b VARCHAR(10)", want: "a int, b varchar(10)",
		},
		{
			name: "a value comma beside a real one",
			in:   "p ENUM('x,y'), b INTEGER", want: "p enum('x,y'), b int",
		},
		{
			name: "decimal beside an enum",
			in:   "d DECIMAL(10,2), p ENUM('a,b')", want: "d decimal(10,2), p enum('a,b')",
		},
		{name: "escaped quote inside a member", in: `p ENUM('x''y,z')`, want: `p enum('x''y,z')`},
		{name: "paren inside a member", in: "p ENUM('a)b')", want: "p enum('a)b')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Check(mysqlroutine.NormalizeParameterList(test.in), qt.Equals, test.want)
		})
	}

	t.Run("the two enum spellings stay distinct", func(t *testing.T) {
		c := qt.New(t)
		c.Check(mysqlroutine.NormalizeParameterList("p ENUM('x,y')"),
			qt.Not(qt.Equals), mysqlroutine.NormalizeParameterList("p ENUM('x','y')"))
	})
}

// TestNormalizeType_IsIdempotent holds the property that makes it safe to run on
// both sides of a comparison: the catalog spelling is already a fixed point, so
// normalizing it again cannot move it.
func TestNormalizeType_IsIdempotent(t *testing.T) {
	for _, in := range []string{
		"integer", "int(11)", "varchar(20)", "decimal(10,2)", "int(10) unsigned",
		"bool", "double precision", "character varying(20)", "text",
	} {
		t.Run(in, func(t *testing.T) {
			c := qt.New(t)
			once := mysqlroutine.NormalizeType(in)
			c.Check(mysqlroutine.NormalizeType(once), qt.Equals, once)
		})
	}
}

// TestNormalizeParameterList_NormalizesTypesAndKeepsNames pins the signature
// rule. The parameter NAME is not a type and must survive untouched, or a
// function whose argument is called `integer` would be renamed by its own
// normalization.
func TestNormalizeParameterList_NormalizesTypesAndKeepsNames(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "single alias", in: "a INTEGER", want: "a int"},
		{name: "catalog width", in: "a int(11)", want: "a int"},
		{name: "two arguments", in: "a INTEGER, b VARCHAR(10)", want: "a int, b varchar(10)"},
		{name: "spacing is normalized", in: "  a   INTEGER ,  b  int(11) ", want: "a int, b int"},
		{name: "multi-word type", in: "a DOUBLE PRECISION", want: "a double"},
		{name: "unsigned survives", in: "a int(10) unsigned", want: "a int unsigned"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Check(mysqlroutine.NormalizeParameterList(test.in), qt.Equals, test.want)
		})
	}
}
