package dbtest

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// atlasScope is what every step translation needs: the file being read, for
// diagnostics, and the evaluation context its expressions resolve against.
//
// The two travel together because they are decided together. A step reached
// through `for_each` is translated once per instance, against a context
// carrying that instance's `each` and `self`, and its diagnostics still name
// the one file every instance came from.
type atlasScope struct {
	filename string
	ctx      *hcl.EvalContext
}

// atlasIteration is one expansion of a `for_each` case: the key and value
// `each` resolves to, and the ordinal that names the instance.
type atlasIteration struct {
	key      cty.Value
	value    cty.Value
	ordinal  int
	expanded bool
}

// atlasEvalOptions carries what a caller knows and the file does not.
type atlasEvalOptions struct {
	devURL string
}

// AtlasTestOption configures how an Atlas `.test.hcl` document is read.
type AtlasTestOption func(*atlasEvalOptions)

// WithAtlasTestDevURL supplies the address `self.dev_url` resolves to.
//
// It is a caller's fact rather than the file's, so a document referring to
// `self.dev_url` without it is refused rather than resolved to an empty string:
// an empty address is a connection string that fails somewhere later, naming
// neither the test nor the omission that produced it.
func WithAtlasTestDevURL(devURL string) AtlasTestOption {
	return func(o *atlasEvalOptions) { o.devURL = devURL }
}

// atlasVariables reads the top-level `variable` blocks into the values `var.*`
// resolves against.
//
// A variable with no `default` is refused. Nothing here supplies one from
// outside yet, so accepting it would bind `var.name` to null and let it reach a
// statement as the string "null" -- a test that runs, passes, and asserts
// against a value nobody wrote.
func atlasVariables(body *hclsyntax.Body, filename string) (map[string]cty.Value, error) {
	values := make(map[string]cty.Value)
	for _, block := range body.Blocks {
		if block.Type != "variable" {
			continue
		}
		if len(block.Labels) != 1 {
			return nil, fmt.Errorf("%s:%d: `variable` needs exactly one label (its name), got %d",
				filename, block.TypeRange.Start.Line, len(block.Labels))
		}
		name := block.Labels[0]
		if err := atlasRejectUnknownAttrs(block, filename, "default", "type"); err != nil {
			return nil, err
		}
		attr, ok := block.Body.Attributes["default"]
		if !ok {
			return nil, fmt.Errorf("%s:%d: variable %q has no `default`, and nothing supplies one",
				filename, block.TypeRange.Start.Line, name)
		}
		value, diags := attr.Expr.Value(nil)
		if diags.HasErrors() {
			return nil, fmt.Errorf("%s:%d: variable %q default: %s",
				filename, attr.SrcRange.Start.Line, name, diags.Error())
		}
		values[name] = value
	}
	return values, nil
}

// atlasEvalContext builds the context one case instance is translated against.
//
// `each` is absent rather than null for a case that does not iterate, so a
// document referring to it there is refused by HCL instead of resolving to
// nothing. The same reasoning puts `self.dev_url` in only when a caller
// supplied one.
func atlasEvalContext(
	variables map[string]cty.Value,
	name string,
	iteration atlasIteration,
	options atlasEvalOptions,
	dir string,
) *hcl.EvalContext {
	self := map[string]cty.Value{"name": cty.StringVal(name)}
	if options.devURL != "" {
		self["dev_url"] = cty.StringVal(options.devURL)
	}

	values := map[string]cty.Value{
		"var":  cty.ObjectVal(orEmptyObject(variables)),
		"self": cty.ObjectVal(self),
	}
	if iteration.expanded {
		values["each"] = cty.ObjectVal(map[string]cty.Value{
			"key":   iteration.key,
			"value": iteration.value,
		})
	}

	return &hcl.EvalContext{
		Variables: values,
		Functions: map[string]function.Function{"file": atlasTestFileFunc(dir)},
	}
}

// orEmptyObject keeps cty.ObjectVal from panicking on a nil map, so a document
// with no `variable` block still resolves `var` to an object that has no
// attributes rather than to nothing at all.
func orEmptyObject(values map[string]cty.Value) map[string]cty.Value {
	if values == nil {
		return make(map[string]cty.Value)
	}
	return values
}

// atlasTestFileFunc binds `file()` to the directory holding the test file.
//
// The confinement is the operating system's: os.Root resolves every component
// in the kernel, so a path leaving the directory is refused whether it leaves
// through an absolute path, a parent traversal, or a symbolic link swapped in
// between the check and the read. That is the same mechanism the project-file
// `file()` relies on for its guarantee; what that one adds is a diagnostic walk
// naming the offending link, and it is deliberately not copied here rather than
// maintained twice -- a second copy could drift in what it explains, and the
// set it refuses is not its to decide.
func atlasTestFileFunc(dir string) function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{{Name: "path", Type: cty.String}},
		Type:   function.StaticReturnType(cty.String),
		Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
			root, err := os.OpenRoot(dir)
			if err != nil {
				return cty.NilVal, fmt.Errorf("file(): open %s: %w", dir, err)
			}
			defer root.Close()

			raw, err := fs.ReadFile(root.FS(), filepath.ToSlash(args[0].AsString()))
			if err != nil {
				return cty.NilVal, fmt.Errorf(
					"file(): %w; a test file reads only inside the directory that holds it", err)
			}
			return cty.StringVal(string(raw)), nil
		},
	})
}

// atlasIterations expands a case's `for_each` into the instances it names.
//
// A case without `for_each` is one instance carrying no `each`. With it, the
// key depends on what was iterated, and the difference is not cosmetic: over a
// collection the key is the ordinal position, and over a mapping it is the
// mapping's own key. A reader who assumed one of those gets the other wrong for
// half the documents that use the feature.
//
// A mapping iterates in sorted key order. Nothing about a written order
// survives into the value HCL hands back, and sorting is what makes a report
// reproducible rather than dependent on how the document happened to be typed.
func atlasIterations(attr *hclsyntax.Attribute, ctx *hcl.EvalContext, filename string) ([]atlasIteration, error) {
	if attr == nil {
		return []atlasIteration{{ordinal: 1}}, nil
	}

	value, diags := attr.Expr.Value(ctx)
	if diags.HasErrors() {
		return nil, fmt.Errorf("%s:%d: `for_each`: %s", filename, attr.SrcRange.Start.Line, diags.Error())
	}
	if value.IsNull() || !value.IsKnown() {
		return nil, fmt.Errorf("%s:%d: `for_each` must be a collection or a mapping, got no value",
			filename, attr.SrcRange.Start.Line)
	}

	valueType := value.Type()
	if valueType.IsMapType() || valueType.IsObjectType() {
		return atlasMappingIterations(value), nil
	}
	if valueType.IsTupleType() || valueType.IsListType() || valueType.IsSetType() {
		return atlasCollectionIterations(value), nil
	}
	return nil, fmt.Errorf("%s:%d: `for_each` must be a collection or a mapping, got %s",
		filename, attr.SrcRange.Start.Line, valueType.FriendlyName())
}

// atlasMappingIterations expands a mapping in sorted key order.
func atlasMappingIterations(value cty.Value) []atlasIteration {
	entries := value.AsValueMap()
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	iterations := make([]atlasIteration, 0, len(keys))
	for i, key := range keys {
		iterations = append(iterations, atlasIteration{
			key:      cty.StringVal(key),
			value:    entries[key],
			ordinal:  i + 1,
			expanded: true,
		})
	}
	return iterations
}

// atlasCollectionIterations expands a collection, keyed by position.
func atlasCollectionIterations(value cty.Value) []atlasIteration {
	elements := value.AsValueSlice()
	iterations := make([]atlasIteration, 0, len(elements))
	for i, element := range elements {
		iterations = append(iterations, atlasIteration{
			key:      cty.NumberIntVal(int64(i)),
			value:    element,
			ordinal:  i + 1,
			expanded: true,
		})
	}
	return iterations
}

// atlasInstanceName is what an expanded case is called.
//
// The ordinal is 1-based and the separator is a slash, so one instance of a
// table-driven case reads as `users/2` wherever a case name appears. An
// unexpanded case keeps the name it was written with, because appending an
// ordinal to a case that has exactly one would make every existing name change
// for no information.
func atlasInstanceName(base string, iteration atlasIteration) string {
	if !iteration.expanded {
		return base
	}
	return fmt.Sprintf("%s/%d", base, iteration.ordinal)
}

// atlasSkip evaluates a case's `skip` expression.
//
// A skipped case is reported as skipped rather than silently dropped, which is
// what makes the attribute readable from the outside: a run whose count of
// cases fell by three says which three, and a `skip` that was true when nobody
// meant it to be shows up as a report line rather than as an absence.
func atlasSkip(attr *hclsyntax.Attribute, ctx *hcl.EvalContext, filename string) (bool, error) {
	if attr == nil {
		return false, nil
	}
	value, diags := attr.Expr.Value(ctx)
	if diags.HasErrors() {
		return false, fmt.Errorf("%s:%d: `skip`: %s", filename, attr.SrcRange.Start.Line, diags.Error())
	}
	if value.IsNull() || !value.IsKnown() || value.Type() != cty.Bool {
		return false, fmt.Errorf("%s:%d: `skip` must be a boolean, got %s",
			filename, attr.SrcRange.Start.Line, value.Type().FriendlyName())
	}
	return value.True(), nil
}

// atlasCaseAttributes are the attributes a `test` block itself takes, as
// opposed to the step blocks in its body.
var atlasCaseAttributes = map[string]bool{"for_each": true, "skip": true}

// atlasCaseContext is what expanding one `test` block needs from the document
// around it.
type atlasCaseContext struct {
	variables map[string]cty.Value
	options   atlasEvalOptions
	dir       string
	filename  string
}

// atlasExpandCase turns one `test` block into the cases it names: one, or one
// per `for_each` element.
//
// Each instance is translated separately rather than once and copied, because
// its steps resolve against its own `each` and `self`. Copying a translated
// case would give every instance the first one's SQL, which is a defect no
// per-instance assertion can see -- the run would report the right number of
// cases, all of them testing the same thing.
func atlasExpandCase(
	block *hclsyntax.Block,
	name string,
	kind AtlasTestKind,
	caseContext atlasCaseContext,
) ([]Case, error) {
	// `for_each` is resolved before any instance exists, so it sees `var` and
	// the file's functions but neither `each` nor an instance's `self`.
	base := atlasEvalContext(
		caseContext.variables, name, atlasIteration{}, caseContext.options, caseContext.dir)
	iterations, err := atlasIterations(
		block.Body.Attributes["for_each"], base, caseContext.filename)
	if err != nil {
		return nil, err
	}

	cases := make([]Case, 0, len(iterations))
	for _, iteration := range iterations {
		instance := atlasInstanceName(name, iteration)
		ctx := atlasEvalContext(
			caseContext.variables, instance, iteration, caseContext.options, caseContext.dir)

		skip, err := atlasSkip(block.Body.Attributes["skip"], ctx, caseContext.filename)
		if err != nil {
			return nil, err
		}

		steps, err := atlasTestSteps(block, atlasScope{filename: caseContext.filename, ctx: ctx}, kind)
		if err != nil {
			return nil, err
		}
		cases = append(cases, Case{Name: instance, Steps: steps, Skip: skip})
	}
	return cases, nil
}
