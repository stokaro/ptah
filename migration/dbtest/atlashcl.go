package dbtest

import (
	"fmt"
	"sort"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// AtlasTestKind selects which family of Atlas `.test.hcl` cases to load.
//
// Atlas labels every case with its kind, `test "schema" "name"` or
// `test "migrate" "name"`, and the two are not interchangeable: a migrate case
// drives the migration directory to a version, which is meaningless in a schema
// test run and would execute migrations the caller did not ask for. Loading is
// therefore always scoped to one kind rather than returning whatever the file
// happens to contain.
type AtlasTestKind string

const (
	// AtlasTestKindSchema selects `test "schema" "..."` cases.
	AtlasTestKindSchema AtlasTestKind = "schema"
	// AtlasTestKindMigrate selects `test "migrate" "..."` cases.
	AtlasTestKindMigrate AtlasTestKind = "migrate"
)

// ParseAtlasTestCases parses an Atlas-format `.test.hcl` document and returns
// the cases of the requested kind, translated to the native [Case] shape.
//
// The Atlas shape is a sequence of labelled `test` blocks whose bodies are
// ordered steps:
//
//	test "schema" "users_insert_select" {
//	  exec {
//	    sql = "INSERT INTO users (id, name) VALUES (1, 'ada')"
//	  }
//	  exec {
//	    sql    = "SELECT name FROM users WHERE id = 1"
//	    output = "ada"
//	  }
//	}
//
// Step order is significant and is preserved: an `exec` that seeds a row must
// run before the `exec` that reads it back. hclsyntax reports blocks in source
// order, so the translation walks Body.Blocks directly rather than using a
// schema-driven decode, which groups blocks by type and would silently reorder
// interleaved steps.
//
// The `output` attribute turns an `exec` into an assertion rather than a bare
// statement, because that is what it means in Atlas: the statement runs and its
// first result is compared. An `exec` without `output` is a plain statement.
func ParseAtlasTestCases(data []byte, filename string, kind AtlasTestKind) ([]Case, error) {
	file, diags := hclsyntax.ParseConfig(data, filename, hcl.Pos{Line: 1, Column: 1})
	if diags.HasErrors() {
		return nil, fmt.Errorf("parse %s: %s", filename, diags.Error())
	}
	body, ok := file.Body.(*hclsyntax.Body)
	if !ok {
		return nil, fmt.Errorf("parse %s: unexpected body type %T", filename, file.Body)
	}

	var cases []Case
	for _, block := range body.Blocks {
		if block.Type != "test" {
			return nil, fmt.Errorf("%s:%d: unsupported block %q: only `test` blocks are supported",
				filename, block.TypeRange.Start.Line, block.Type)
		}
		if len(block.Labels) != 2 {
			return nil, fmt.Errorf("%s:%d: `test` needs exactly two labels (kind and name), got %d",
				filename, block.TypeRange.Start.Line, len(block.Labels))
		}
		blockKind, name := AtlasTestKind(block.Labels[0]), block.Labels[1]
		switch blockKind {
		case AtlasTestKindSchema, AtlasTestKindMigrate:
		default:
			return nil, fmt.Errorf("%s:%d: unsupported test kind %q: want %q or %q",
				filename, block.TypeRange.Start.Line, blockKind,
				AtlasTestKindSchema, AtlasTestKindMigrate)
		}
		if blockKind != kind {
			continue
		}

		steps, err := atlasTestSteps(block, filename)
		if err != nil {
			return nil, err
		}
		cases = append(cases, Case{Name: name, Steps: steps})
	}

	if err := validateCases(cases); err != nil {
		return nil, err
	}
	return cases, nil
}

// atlasTestSteps translates one `test` block body into ordered native steps.
func atlasTestSteps(block *hclsyntax.Block, filename string) ([]Step, error) {
	if len(block.Body.Attributes) > 0 {
		names := make([]string, 0, len(block.Body.Attributes))
		for attrName := range block.Body.Attributes {
			names = append(names, attrName)
		}
		sort.Strings(names)
		return nil, fmt.Errorf("%s:%d: `test` body takes step blocks, not attributes: %v",
			filename, block.TypeRange.Start.Line, names)
	}

	steps := make([]Step, 0, len(block.Body.Blocks))
	for _, step := range block.Body.Blocks {
		line := step.TypeRange.Start.Line
		switch step.Type {
		case "exec":
			sql, err := atlasRequiredString(step, "sql", filename)
			if err != nil {
				return nil, err
			}
			output, hasOutput, err := atlasOptionalString(step, "output", filename)
			if err != nil {
				return nil, err
			}
			if err := atlasRejectUnknownAttrs(step, filename, "sql", "output"); err != nil {
				return nil, err
			}
			if hasOutput {
				steps = append(steps, Step{Assert: &Assertion{Query: sql, Scalar: &output}})
				continue
			}
			steps = append(steps, Step{Exec: sql})
		case "migrate":
			to, err := atlasRequiredString(step, "to", filename)
			if err != nil {
				return nil, err
			}
			if err := atlasRejectUnknownAttrs(step, filename, "to"); err != nil {
				return nil, err
			}
			steps = append(steps, Step{MigrateTo: to})
		default:
			return nil, fmt.Errorf("%s:%d: unsupported step %q: want `exec` or `migrate`",
				filename, line, step.Type)
		}
	}
	return steps, nil
}

func atlasRequiredString(block *hclsyntax.Block, name, filename string) (string, error) {
	value, ok, err := atlasOptionalString(block, name, filename)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("%s:%d: `%s` requires %s",
			filename, block.TypeRange.Start.Line, block.Type, name)
	}
	return value, nil
}

func atlasOptionalString(block *hclsyntax.Block, name, filename string) (string, bool, error) {
	attr, ok := block.Body.Attributes[name]
	if !ok {
		return "", false, nil
	}
	value, diags := attr.Expr.Value(nil)
	if diags.HasErrors() {
		return "", false, fmt.Errorf("%s:%d: %s: %s",
			filename, attr.NameRange.Start.Line, name, diags.Error())
	}
	if value.Type() != cty.String {
		return "", false, fmt.Errorf("%s:%d: %s must be a string",
			filename, attr.NameRange.Start.Line, name)
	}
	return value.AsString(), true, nil
}

// atlasRejectUnknownAttrs fails on an attribute the step does not define.
//
// Unlike atlas.hcl itself, which tolerates unknown names, a test file is our own
// ingestion surface: a typo in `output` would silently downgrade an assertion to
// a bare statement, and the case would pass while checking nothing.
func atlasRejectUnknownAttrs(block *hclsyntax.Block, filename string, allowed ...string) error {
	known := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		known[name] = true
	}
	var unknown []string
	for name := range block.Body.Attributes {
		if !known[name] {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf("%s:%d: `%s` does not take %v: want %v",
		filename, block.TypeRange.Start.Line, block.Type, unknown, allowed)
}
