package projectconfig

import (
	"fmt"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

var atlasLazyDataSourceTypes = map[string]struct{}{
	"aws_rds_token":      {},
	"external":           {},
	"external_schema":    {},
	"gcp_cloudsql_token": {},
	"hcl_schema":         {},
	"remote_dir":         {},
	"remote_schema":      {},
	"runtimevar":         {},
	"sql":                {},
	"template_dir":       {},
}

type atlasDataSourceKey struct {
	typ  string
	name string
}

func (k atlasDataSourceKey) String() string {
	return "data." + k.typ + "." + k.name
}

type atlasEvalState uint8

const (
	atlasEvalUnvisited atlasEvalState = iota
	atlasEvalVisiting
	atlasEvalComplete
)

type atlasEvaluator struct {
	parser      *atlasParser
	localAttrs  hclsyntax.Attributes
	localValues map[string]cty.Value
	localState  map[string]atlasEvalState
	dataBlocks  map[atlasDataSourceKey]*hclsyntax.Block
	dataOrder   []atlasDataSourceKey
	dataValues  map[string]map[string]cty.Value
	dataState   map[atlasDataSourceKey]atlasEvalState
}

func (p atlasParser) configureEvalContext(
	variableBlocks []*hclsyntax.Block,
	localBlocks []*hclsyntax.Block,
	dataBlocks []*hclsyntax.Block,
	roots []hclsyntax.Expression,
) error {
	if err := p.configureVariables(variableBlocks); err != nil {
		return err
	}
	evaluator, err := newAtlasEvaluator(&p, localBlocks, dataBlocks)
	if err != nil {
		return err
	}
	for _, name := range sortedAttributeNames(evaluator.localAttrs) {
		if err := evaluator.resolveLocal(name); err != nil {
			return err
		}
	}
	if err := evaluator.validateHCLSchemaVariables(); err != nil {
		return err
	}
	for _, expr := range roots {
		if err := evaluator.resolveExpressionDependencies(expr); err != nil {
			return err
		}
	}
	return nil
}

func newAtlasEvaluator(
	parser *atlasParser,
	localBlocks []*hclsyntax.Block,
	dataBlocks []*hclsyntax.Block,
) (*atlasEvaluator, error) {
	evaluator := &atlasEvaluator{
		parser:      parser,
		localAttrs:  hclsyntax.Attributes{},
		localValues: map[string]cty.Value{},
		localState:  map[string]atlasEvalState{},
		dataBlocks:  map[atlasDataSourceKey]*hclsyntax.Block{},
		dataOrder:   make([]atlasDataSourceKey, 0, len(dataBlocks)),
		dataValues:  map[string]map[string]cty.Value{},
		dataState:   map[atlasDataSourceKey]atlasEvalState{},
	}
	if err := evaluator.collectLocals(localBlocks); err != nil {
		return nil, err
	}
	if err := evaluator.collectDataSources(dataBlocks); err != nil {
		return nil, err
	}
	return evaluator, nil
}

func (e *atlasEvaluator) collectLocals(blocks []*hclsyntax.Block) error {
	for _, block := range blocks {
		if len(block.Labels) > 0 {
			return unsupportedBlock(block)
		}
		if len(block.Body.Blocks) > 0 {
			return unsupportedBlock(block.Body.Blocks[0])
		}
		for name, attr := range block.Body.Attributes {
			if _, ok := e.localAttrs[name]; ok {
				return fmt.Errorf(
					"duplicate atlas.hcl local %q at %s:%d",
					name,
					attr.NameRange.Filename,
					attr.NameRange.Start.Line,
				)
			}
			e.localAttrs[name] = attr
		}
	}
	return nil
}

func (e *atlasEvaluator) collectDataSources(blocks []*hclsyntax.Block) error {
	for _, block := range blocks {
		if len(block.Labels) != 2 {
			return unsupportedBlock(block)
		}
		if _, ok := atlasLazyDataSourceTypes[block.Labels[0]]; !ok {
			return unsupported(block.Type+"."+block.Labels[0], block.TypeRange)
		}
		key := atlasDataSourceKey{typ: block.Labels[0], name: block.Labels[1]}
		if _, ok := e.dataBlocks[key]; ok {
			return fmt.Errorf(
				"duplicate atlas.hcl %s %q at %s:%d",
				"data."+key.typ,
				key.name,
				block.TypeRange.Filename,
				block.TypeRange.Start.Line,
			)
		}
		if err := validateAtlasDataSourceDeclarationShape(block); err != nil {
			return err
		}
		e.dataBlocks[key] = block
		e.dataOrder = append(e.dataOrder, key)
	}
	return nil
}

// validateHCLSchemaVariables decodes the variables declared by hcl_schema
// blocks without resolving their paths. Atlas validates this value shape while
// loading the project even when no selected environment uses the data source,
// but an unused path must remain lazy and may name a file that is not present
// in this run.
func (e *atlasEvaluator) validateHCLSchemaVariables() error {
	for _, key := range e.dataOrder {
		if key.typ != "hcl_schema" {
			continue
		}
		attr := e.dataBlocks[key].Body.Attributes["vars"]
		if attr == nil {
			continue
		}
		if err := e.resolveExpressionDependencies(attr.Expr); err != nil {
			return err
		}
		if _, err := e.parser.hclSchemaVarsAttr(attr); err != nil {
			return err
		}
	}
	return nil
}

func (e *atlasEvaluator) resolveExpressionDependencies(expr hclsyntax.Expression) error {
	for _, traversal := range expr.Variables() {
		switch traversal.RootName() {
		case "local":
			name, ok := atlasTraversalAttribute(traversal, 1)
			if ok {
				if err := e.resolveLocal(name); err != nil {
					return err
				}
			}
		case "data":
			typ, typeOK := atlasTraversalAttribute(traversal, 1)
			name, nameOK := atlasTraversalAttribute(traversal, 2)
			if typeOK && nameOK {
				if err := e.resolveDataSource(atlasDataSourceKey{typ: typ, name: name}); err != nil {
					return err
				}
			}
		}
	}
	return e.resolveComputedDataSourceIndices(expr)
}

func (e *atlasEvaluator) resolveComputedDataSourceIndices(expr hclsyntax.Expression) error {
	indices := make([]*hclsyntax.IndexExpr, 0, 1)
	hclsyntax.VisitAll(expr, func(node hclsyntax.Node) hcl.Diagnostics {
		if index, ok := node.(*hclsyntax.IndexExpr); ok {
			indices = append(indices, index)
		}
		return nil
	})
	for _, index := range indices {
		key, ok := computedDataSourceKey(index, e.parser.ctx)
		if !ok {
			continue
		}
		if err := e.resolveDataSource(key); err != nil {
			return err
		}
	}
	return nil
}

func computedDataSourceKey(index *hclsyntax.IndexExpr, ctx *hcl.EvalContext) (atlasDataSourceKey, bool) {
	traversal, diags := hcl.AbsTraversalForExpr(index.Collection)
	if diags.HasErrors() || len(traversal) != 2 || traversal.RootName() != "data" {
		return atlasDataSourceKey{}, false
	}
	typ, ok := atlasTraversalAttribute(traversal, 1)
	if !ok {
		return atlasDataSourceKey{}, false
	}
	value, diags := index.Key.Value(ctx)
	if diags.HasErrors() || !value.IsKnown() || value.IsNull() {
		return atlasDataSourceKey{}, false
	}
	value, _ = value.Unmark()
	if !value.Type().Equals(cty.String) {
		return atlasDataSourceKey{}, false
	}
	return atlasDataSourceKey{typ: typ, name: value.AsString()}, true
}

func atlasTraversalAttribute(traversal hcl.Traversal, index int) (string, bool) {
	if len(traversal) <= index {
		return "", false
	}
	attr, ok := traversal[index].(hcl.TraverseAttr)
	return attr.Name, ok
}

func (e *atlasEvaluator) resolveLocal(name string) error {
	attr, ok := e.localAttrs[name]
	if !ok {
		return nil
	}
	switch e.localState[name] {
	case atlasEvalComplete:
		return nil
	case atlasEvalVisiting:
		return fmt.Errorf("atlas.hcl evaluation cycle involving local.%s", name)
	}
	e.localState[name] = atlasEvalVisiting
	if err := e.resolveExpressionDependencies(attr.Expr); err != nil {
		return err
	}
	value, diags := attr.Expr.Value(e.parser.ctx)
	if diags.HasErrors() {
		return e.parser.evaluationFailed(name, attr, diags)
	}
	e.localValues[name] = value
	e.parser.ctx.Variables["local"] = cty.ObjectVal(e.localValues)
	e.localState[name] = atlasEvalComplete
	return nil
}

func (e *atlasEvaluator) resolveDataSource(key atlasDataSourceKey) error {
	block, ok := e.dataBlocks[key]
	if !ok {
		return fmt.Errorf("atlas.hcl %s is not declared", key)
	}
	switch e.dataState[key] {
	case atlasEvalComplete:
		return nil
	case atlasEvalVisiting:
		return fmt.Errorf("atlas.hcl evaluation cycle involving %s", key)
	}
	if err := validateAtlasDataSourceShape(block); err != nil {
		return err
	}
	e.dataState[key] = atlasEvalVisiting
	for _, expr := range atlasBodyExpressions(block.Body) {
		if err := e.resolveExpressionDependencies(expr); err != nil {
			return err
		}
	}
	value, err := e.parser.resolveAtlasDataSource(block)
	if err != nil {
		return err
	}
	values := e.dataValues[key.typ]
	if values == nil {
		values = map[string]cty.Value{}
		e.dataValues[key.typ] = values
	}
	values[key.name] = value
	e.updateDataContext()
	e.dataState[key] = atlasEvalComplete
	return nil
}

func (e *atlasEvaluator) updateDataContext() {
	types := make(map[string]cty.Value, len(e.dataValues))
	for typ, values := range e.dataValues {
		types[typ] = cty.ObjectVal(values)
	}
	e.parser.ctx.Variables["data"] = cty.ObjectVal(types)
}

func atlasBodyExpressions(body *hclsyntax.Body) []hclsyntax.Expression {
	expressions := make([]hclsyntax.Expression, 0, len(body.Attributes))
	for _, name := range sortedAttributeNames(body.Attributes) {
		expressions = append(expressions, body.Attributes[name].Expr)
	}
	for _, block := range body.Blocks {
		expressions = append(expressions, atlasBodyExpressions(block.Body)...)
	}
	return expressions
}

func atlasEvaluationRoots(
	topAttrs hclsyntax.Attributes,
	globalDiff []*hclsyntax.Block,
	globalLint []*hclsyntax.Block,
	selected []atlasEnvBlock,
) []hclsyntax.Expression {
	roots := make([]hclsyntax.Expression, 0, len(topAttrs))
	for _, name := range sortedAttributeNames(topAttrs) {
		roots = append(roots, topAttrs[name].Expr)
	}
	for _, block := range append(append([]*hclsyntax.Block{}, globalDiff...), globalLint...) {
		roots = append(roots, atlasBodyExpressions(block.Body)...)
	}
	for _, env := range selected {
		roots = append(roots, atlasBodyExpressions(env.block.Body)...)
	}
	return roots
}
