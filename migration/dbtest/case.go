// Package dbtest is a Ptah-native, declarative migration test runner.
//
// It applies a project's migrations to an ephemeral (or explicitly throwaway)
// database and asserts on the result, so migration behavior can be exercised in
// CI without a bespoke test harness. Test cases are authored either in Go using
// the exported [Case]/[Step]/[Assertion] types, or as YAML loaded with
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
// # Phase 1 scope
//
// Phase 1 supports only the migrate_to, exec, and assert step kinds and the
// row_count, scalar, and error_contains assertions listed above. Seed steps,
// desired-schema application, and structured (HTML/JSON) reporting are out of
// scope. Reporting is text only via [Report.Text].
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
	// Assert runs a query and checks a single condition on the result.
	Assert *Assertion `yaml:"assert"`
}

// Assertion runs Query and checks exactly one of RowCount, Scalar, or
// ErrorContains.
type Assertion struct {
	// Query is the SQL executed by the assertion. It is always required.
	Query string `yaml:"query"`
	// RowCount asserts that Query returns exactly this many rows.
	RowCount *int `yaml:"row_count"`
	// Scalar asserts that the first column of the first row of Query, formatted
	// as a string, equals this value.
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
	stepKindAssert
)

// kind reports which single action the step performs and how many actions are
// set. A well-formed step has exactly one action set.
func (s Step) kind() (kind stepKind, setCount int) {
	if s.MigrateTo != "" {
		kind = stepKindMigrateTo
		setCount++
	}
	if s.Exec != "" {
		kind = stepKindExec
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
		return fmt.Errorf("step must set exactly one of migrate_to, exec, or assert, but none is set")
	}
	if setCount > 1 {
		return fmt.Errorf("step must set exactly one of migrate_to, exec, or assert, but %d are set", setCount)
	}
	if s.Assert != nil {
		return s.Assert.validate()
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
