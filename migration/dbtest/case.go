// Package dbtest is a Ptah-native, declarative database test runner.
//
// It provides two entry points that share the same [Case]/[Step]/[Assertion]
// model and database isolation:
//
//   - [RunMigrationTest] applies a project's migrations to an ephemeral (or
//     explicitly throwaway) database and asserts on the result, so migration
//     behavior can be exercised in CI without a bespoke test harness.
//   - [RunSchemaTest] applies a desired schema (rendered from Go entity
//     annotations) to a fresh database once per case and then asserts on the
//     result, so a schema definition can be exercised without authoring
//     migrations.
//
// Test cases are authored either in Go using the exported
// [Case]/[Step]/[Assertion] types, or as YAML loaded with
// [ParseCases]/[LoadCases].
//
// # YAML format
//
// A test file is a YAML document with a top-level cases: list. Each case has a
// name and an ordered list of steps. A step performs exactly one action:
//
//   - migrate_to: migrate the database to a target version. The value is an
//     integer version, the string "latest" (migrate up to the newest
//     migration), or the string "0" (roll everything back).
//   - exec: run raw SQL against the database.
//   - seed: apply environment-scoped SQL seed files from a directory.
//   - assert: run a query and check exactly one condition (row_count, scalar,
//     or error_contains).
//
// Example:
//
//	cases:
//	  - name: products table accepts rows
//	    steps:
//	      - name: migrate to latest
//	        migrate_to: latest
//	      - name: insert a product
//	        exec: INSERT INTO products (name) VALUES ('widget')
//	      - name: exactly one product exists
//	        assert:
//	          query: SELECT id FROM products
//	          row_count: 1
//	      - name: the product is named widget
//	        assert:
//	          query: SELECT name FROM products LIMIT 1
//	          scalar: widget
//	      - name: unknown table errors
//	        assert:
//	          query: SELECT * FROM does_not_exist
//	          error_contains: does_not_exist
//
// # Schema tests
//
// [RunSchemaTest] reuses the same step and assertion vocabulary but applies a
// desired schema instead of migrations. A migrate_to step is invalid in a
// schema test — there are no migrations to move between — and fails with an
// explanatory detail rather than being silently skipped.
//
// # Scope
//
// The supported step kinds are migrate_to (migration tests only), exec, seed,
// and assert, with the row_count, scalar, and error_contains assertions listed
// above. Structured (HTML/JSON) reporting is out of scope; reporting is text
// only via [Report.Text].
//
// # Database isolation
//
// In the default (ephemeral) mode — no database URL supplied — each case runs
// against its own fresh SQLite database, so state created by one case is never
// visible to another. When an explicit throwaway database URL is supplied, all
// cases share that one database and the caller is responsible for keeping them
// independent.
package dbtest

import (
	"fmt"
	"strings"
)

// Case is a single declarative migration test: a named, ordered list of steps
// executed against the test database.
type Case struct {
	Name  string `yaml:"name"`
	Steps []Step `yaml:"steps"`
}

// Step performs exactly one action against the test database. Exactly one of
// MigrateTo, Exec, or Assert must be set.
type Step struct {
	// Name is a human-readable label used in reporting. It is optional.
	Name string `yaml:"name"`
	// MigrateTo migrates the database to a target version. It accepts an integer
	// version, "latest", or "0".
	MigrateTo string `yaml:"migrate_to"`
	// Exec runs raw SQL against the database.
	Exec string `yaml:"exec"`
	// Seed applies environment-scoped SQL seed files from a directory.
	Seed *SeedStep `yaml:"seed"`
	// Assert runs a query and checks a single condition on the result.
	Assert *Assertion `yaml:"assert"`
}

// SeedStep applies SQL seed files from Dir using the seeder's
// NNN_description.env.sql convention: files matching Env plus files ending in
// .all.sql are applied in version order. Because the target is a throwaway test
// database, protected-environment and protected-table guards do not apply.
type SeedStep struct {
	// Dir is the directory of seed files. It is required.
	Dir string `yaml:"dir"`
	// Env is the seed environment to apply (for example dev or test). It is
	// required; files matching Env plus files ending in .all.sql are applied.
	Env string `yaml:"env"`
}

// Assertion runs Query and checks exactly one of RowCount, Scalar, or
// ErrorContains.
type Assertion struct {
	// Query is the SQL executed by the assertion. It is always required and
	// should be a SELECT: with row_count and scalar it is run as a query, and
	// with error_contains it is run and expected to fail, so a non-SELECT
	// statement would execute its side effects.
	Query string `yaml:"query"`
	// RowCount asserts that Query returns exactly this many rows.
	RowCount *int `yaml:"row_count"`
	// Scalar asserts that the first column of the first row of Query, formatted
	// as a string, equals this value. Values are formatted deterministically:
	// []byte and text as their string, time.Time as RFC3339, SQL NULL as
	// "<nil>", and other types via fmt's default. Select a column as text (for
	// example CAST(col AS TEXT)) to compare its raw stored form.
	Scalar *string `yaml:"scalar"`
	// ErrorContains asserts that running Query fails with an error message that
	// contains this substring.
	ErrorContains string `yaml:"error_contains"`
}

// stepKind classifies which action a [Step] performs.
type stepKind int

const (
	stepKindNone stepKind = iota
	stepKindMigrateTo
	stepKindExec
	stepKindSeed
	stepKindAssert
)

// kind reports which single action the step performs and how many actions are
// set. A well-formed step has exactly one action set.
func (s Step) kind() (kind stepKind, setCount int) {
	if strings.TrimSpace(s.MigrateTo) != "" {
		kind = stepKindMigrateTo
		setCount++
	}
	if strings.TrimSpace(s.Exec) != "" {
		kind = stepKindExec
		setCount++
	}
	if s.Seed != nil {
		kind = stepKindSeed
		setCount++
	}
	if s.Assert != nil {
		kind = stepKindAssert
		setCount++
	}
	return kind, setCount
}

// validateCases validates a full set of cases, returning the first error found.
func validateCases(cases []Case) error {
	for i := range cases {
		if err := cases[i].validate(); err != nil {
			return fmt.Errorf("case %d: %w", i+1, err)
		}
	}
	return nil
}

func (c Case) validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("test case has no name")
	}
	if len(c.Steps) == 0 {
		return fmt.Errorf("test case %q has no steps", c.Name)
	}
	for i := range c.Steps {
		if err := c.Steps[i].validate(); err != nil {
			return fmt.Errorf("test case %q, step %d: %w", c.Name, i+1, err)
		}
	}
	return nil
}

func (s Step) validate() error {
	_, setCount := s.kind()
	if setCount == 0 {
		return fmt.Errorf("step must set exactly one of migrate_to, exec, seed, or assert, but none is set")
	}
	if setCount > 1 {
		return fmt.Errorf("step must set exactly one of migrate_to, exec, seed, or assert, but %d are set", setCount)
	}
	if s.Seed != nil {
		return s.Seed.validate()
	}
	if s.Assert != nil {
		return s.Assert.validate()
	}
	return nil
}

func (s *SeedStep) validate() error {
	if strings.TrimSpace(s.Dir) == "" {
		return fmt.Errorf("seed requires a dir")
	}
	if strings.TrimSpace(s.Env) == "" {
		return fmt.Errorf("seed requires an env")
	}
	return nil
}

func (a *Assertion) validate() error {
	if strings.TrimSpace(a.Query) == "" {
		return fmt.Errorf("assert requires a query")
	}
	setCount := 0
	if a.RowCount != nil {
		setCount++
	}
	if a.Scalar != nil {
		setCount++
	}
	if a.ErrorContains != "" {
		setCount++
	}
	if setCount == 0 {
		return fmt.Errorf("assert must set exactly one of row_count, scalar, or error_contains, but none is set")
	}
	if setCount > 1 {
		return fmt.Errorf("assert must set exactly one of row_count, scalar, or error_contains, but %d are set", setCount)
	}
	return nil
}
