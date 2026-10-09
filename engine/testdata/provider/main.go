// This program is copied into an independent module by the external-provider
// test. All Ptah imports are public contracts, and no built-in target is linked.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing/fstest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/migration/importer"
)

type widget struct {
	Name   string   `json:"name"`
	Levels []string `json:"levels"`
	Tables []string `json:"tables"`
}

func (*widget) Kind() schemaext.Kind { return "example.org/widget/model" }
func (v *widget) Clone() schemaext.Value {
	return &widget{Name: v.Name, Levels: slices.Clone(v.Levels), Tables: slices.Clone(v.Tables)}
}
func (v *widget) Equal(other schemaext.Value) bool {
	w, ok := other.(*widget)
	return ok && v.Name == w.Name && slices.Equal(v.Levels, w.Levels) && slices.Equal(v.Tables, w.Tables)
}

type addWidget struct{ Name string }

func (*addWidget) Kind() schemaext.Kind                   { return "example.org/widget/add" }
func (p *addWidget) CloneExtension() ast.ExtensionPayload { return &addWidget{Name: p.Name} }

type service struct{ extensions renderer.Extensions }

func (s service) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	result := renderer.Result{Complete: true}
	for index, node := range request.Nodes {
		if err := ctx.Err(); err != nil {
			return renderer.Result{}, err
		}
		statement, ok := node.(*ast.ExtensionStatement)
		if !ok {
			return renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{
				Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: "node", Feature: "statement", Message: fmt.Sprintf("unsupported node %T", node)},
				Input:   new(index),
			}}}, nil
		}
		sql, err := s.extensions.Render(renderer.ExtensionContext{Target: request.Target}, ast.StatementExtension, statement.Payload)
		if err != nil {
			return renderer.Result{}, err
		}
		fragment := ""
		for _, text := range sql {
			fragment += text + "\n"
		}
		result.Fragments = append(result.Fragments, fragment)
	}
	return result, nil
}

func codec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	return schemaext.Codec{
		Prototype: &widget{}, Representation: schemaext.Desired, Version: 1,
		Definition: json.RawMessage(`{"name":"string","levels":"ordered string list","tables":"ordered table reference list"}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, ok := payload.(*widget)
			if !ok {
				return nil, fmt.Errorf("unexpected model %T", payload)
			}
			return value.Clone(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*widget](data) },
	}
}

func operationCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	return schemaext.Codec{
		Prototype: &addWidget{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"Name":"string"}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			return payload.(*addWidget).CloneExtension(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*addWidget](data) },
	}
}

// grantWidget is an access-control operation. Its owner computes the access
// assessment while it still holds the captured context and carries the result
// as data, so the assessment crosses the codec boundary with the operation.
type grantWidget struct {
	Role   string                 `json:"role"`
	Access schemaext.AccessEffect `json:"access"`
}

func (*grantWidget) Kind() schemaext.Kind { return "example.org/widget/grant" }
func (p *grantWidget) CloneExtension() ast.ExtensionPayload {
	cloned := *p
	return &cloned
}
func (p *grantWidget) AccessEffect() schemaext.AccessEffect { return p.Access }

func grantCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	return schemaext.Codec{
		Prototype:      &grantWidget{Access: schemaext.AccessEffect{Access: schemaext.AccessUnknown, Reason: "prototype"}},
		Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["role","access"],"additionalProperties":false,"properties":{"role":{"type":"string"},"access":` +
			string(schemaext.AccessEffectSchema()) + `}}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			return payload.(*grantWidget).CloneExtension(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[*grantWidget](data) },
	}
}

func main() {
	if err := verify(); err != nil {
		panic(err)
	}
	fmt.Println("external provider conformance passed")
}

func verify() error {
	handlers, err := renderer.NewExtensions(renderer.TypedHandler(&addWidget{}, ast.StatementExtension,
		func(renderer.ExtensionContext, *addWidget) error { return nil },
		func(_ renderer.ExtensionContext, operation *addWidget) ([]string, error) {
			return []string{"CREATE WIDGET " + operation.Name}, nil
		}))
	if err != nil {
		return err
	}
	observed := codec()
	observed.Representation = schemaext.Observed
	runtime, err := engine.New(engine.Provider{ID: "example.org/widget", Codecs: []schemaext.Codec{codec(), observed, operationCodec(), grantCodec()},
		Planning: []engine.Planning{{Target: "widget", ParentKinds: []schemaext.Kind{(&widget{}).Kind()}, OperationKinds: []schemaext.Kind{(&addWidget{}).Kind()}, Service: service{}}},
		Relations: []engine.RelationDiscovery{{Target: "widget", Representation: schemaext.Desired,
			Kinds: []schemaext.Kind{(&widget{}).Kind()}, Service: service{}}},
		Targets: []engine.Target{{Name: "widget", Rendering: service{extensions: handlers}, Validation: service{}, SchemaRendering: service{}, Creations: service{}}}})
	if err != nil {
		return err
	}
	result, err := runtime.Render(context.Background(), renderer.Request{Target: "widget", Nodes: []ast.Node{
		&ast.ExtensionStatement{Payload: &addWidget{Name: "sample"}},
	}})
	if err != nil {
		return err
	}
	if result.SQL() != "CREATE WIDGET sample\n" {
		return fmt.Errorf("wrong custom render: %q", result.SQL())
	}
	if err := verifyBatchRefusal(runtime); err != nil {
		return err
	}
	_, err = runtime.Render(context.Background(), renderer.Request{Target: "postgres"})
	if !errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		return fmt.Errorf("unselected target was not refused: %v", err)
	}
	original := &widget{Name: "sample", Levels: []string{"two", "one"}}
	facets, err := schemaext.NewFacets(original)
	if err != nil {
		return err
	}
	encoded, err := runtime.Codecs().EncodeFacets(context.Background(), schemaext.Desired, facets)
	if err != nil {
		return err
	}
	decoded, err := runtime.Codecs().DecodeFacets(context.Background(), schemaext.Desired, encoded)
	if err != nil {
		return err
	}
	value, found, err := schemaext.FacetAs[*widget](decoded, original.Kind())
	if err != nil {
		return err
	}
	if !found || !value.Equal(original) {
		return fmt.Errorf("typed round trip lost the value")
	}
	if err := verifyValidation(runtime, decoded); err != nil {
		return err
	}
	if err := verifySchemaRendering(runtime, decoded); err != nil {
		return err
	}
	if err := verifyTargetScope(decoded); err != nil {
		return err
	}
	if err := verifyCreationProjection(runtime); err != nil {
		return err
	}
	if err := verifyPlanningRefusal(runtime); err != nil {
		return err
	}
	if err := verifyCommonRewrite(runtime); err != nil {
		return err
	}
	if err := verifyRelations(runtime); err != nil {
		return err
	}
	if err := verifyImport(); err != nil {
		return err
	}
	if err := verifyAccessEffects(runtime); err != nil {
		return err
	}
	value.Levels[0] = "changed"
	if original.Levels[0] != "two" {
		return fmt.Errorf("round trip exposed aliased state")
	}
	return nil
}

func (service) DescribeRelations(ctx context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
	result := schemaext.RelationResult{Complete: true}
	b := objectidentity.NewBuilder(request.Identifiers)
	for _, input := range request.Values {
		if err := ctx.Err(); err != nil {
			return schemaext.RelationResult{}, err
		}
		value, ok := input.Value.(*widget)
		if !ok {
			return schemaext.RelationResult{}, fmt.Errorf("unexpected relation model %T", input.Value)
		}
		record := schemaext.ValueRelations{Subject: input.Subject, Complete: true}
		for _, table := range value.Tables {
			record.Dependencies = append(record.Dependencies, b.Table(table))
		}
		result.Values = append(result.Values, record)
	}
	return result, nil
}

func verifyRelations(runtime *engine.Runtime) error {
	ctx := context.Background()
	semantics := identifier.ForDialect("postgres")
	b := objectidentity.NewBuilder(semantics)
	value := &widget{Name: "shared", Tables: []string{"orders", "tenants"}}
	var kinds []schemaext.KindCoverage
	for _, model := range runtime.Codecs().Definitions() {
		if model.Representation == schemaext.Desired {
			kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
		}
	}
	coverage, err := schemaext.NewCoverage(schemaext.Desired, kinds, nil)
	if err != nil {
		return err
	}
	request := schemaext.RelationRequest{Target: "widget", Representation: schemaext.Desired, Identifiers: semantics, Coverage: coverage,
		Values: []schemaext.RelationValue{{Subject: schemaext.RelationSubject{Kind: value.Kind(), Placement: schemaext.ObjectPlacement,
			Subject: b.SchemaScopedParts(objectidentity.Kind(value.Kind()), "security", value.Name)}, Value: value}}}
	snapshot, err := runtime.CaptureRelations(ctx, request)
	if err != nil {
		return err
	}
	related, err := snapshot.CaptureRelated(ctx, []objectidentity.ID{b.Table("orders")})
	if err != nil {
		return err
	}
	if len(related.Values) != 1 || !related.Values[0].Value.Equal(value) || len(related.Required) != 2 ||
		!slices.Contains(related.Required, b.Table("orders")) || !slices.Contains(related.Required, b.Table("tenants")) {
		return fmt.Errorf("related capture lost or split a multi-table object")
	}
	request.Coverage = schemaext.Coverage{}
	snapshot, err = runtime.CaptureRelations(ctx, request)
	if err != nil {
		return err
	}
	_, err = snapshot.CaptureRelated(ctx, []objectidentity.ID{b.Table("orders")})
	if !errors.Is(err, schemaext.ErrIncompleteRelations) {
		return fmt.Errorf("relation registration manufactured source coverage: %v", err)
	}
	return nil
}

func (service) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	for index, table := range request.Tables {
		if table.Action == featureplan.AlterTable {
			return planCommonRewrite(request, table)
		}
		if table.Action != "" {
			return featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
				Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string((&widget{}).Kind()),
					Object: table.Subject.String(), Feature: "widget-history", Message: "widget history cannot be reconstructed"},
				Parent: new(index),
			}}}, nil
		}
	}
	return featureplan.Result{Complete: true}, nil
}

func planCommonRewrite(request featureplan.Request, table featureplan.Table) (featureplan.Result, error) {
	if len(request.CommonSteps) != 1 || request.CommonSteps[0].AddedColumn == nil {
		return featureplan.Result{}, fmt.Errorf("a widget rewrite requires the accepted column addition")
	}
	common := request.CommonSteps[0]
	id := plangraph.StepID{Owner: "example.org/widget", Name: "column-with-widget"}
	return featureplan.Result{
		Complete: true,
		Contributions: []plangraph.Contribution[featureplan.Operation]{{Owner: id.Owner, Steps: []plangraph.Step[featureplan.Operation]{{
			ID: id, Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &addWidget{Name: common.AddedColumn.Name}},
			Effects: slices.Clone(common.Effects), Transaction: plangraph.TransactionForbidden,
			Impact: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "the combined operation changes a widget and its column"},
		}}}},
		Parents:  []featureplan.ParentPlan{{Subject: table.Subject, Kind: (&widget{}).Kind(), Action: table.Action, Strategy: "add the column and widget in one owner operation", Steps: []plangraph.StepID{id}}},
		Rewrites: []plangraph.Rewrite{{Sources: []plangraph.StepID{common.ID}, Replacement: id}},
	}, nil
}

func verifyCommonRewrite(runtime *engine.Runtime) error {
	ctx := context.Background()
	semantics := identifier.ForDialect("widget")
	builder := objectidentity.NewBuilder(semantics)
	parent := builder.Table("items")
	common := featureplan.CommonStep{
		ID: plangraph.StepID{Owner: "example.org/common", Name: "add-extra"}, Parent: parent,
		Effects: []plangraph.Effect{{Subject: builder.Column("items", "extra"), Action: plangraph.Create}}, AddedColumn: ast.NewColumn("extra", "INTEGER"),
	}
	request := featureplan.Request{Target: "widget", Identifiers: semantics, CommonSteps: []featureplan.CommonStep{common}, Tables: []featureplan.Table{{
		Subject: parent, Action: featureplan.AlterTable,
		Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}},
		Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "items"}},
	}}}
	result, err := runtime.PlanFeatures(ctx, request)
	if err != nil {
		return err
	}
	if err := result.Err(request); err != nil {
		return err
	}
	if len(result.Contributions) != 1 || len(result.Contributions[0].Steps) != 1 || len(result.Rewrites) != 1 {
		return fmt.Errorf("rewrite planning returned incomplete receipts")
	}
	// A process adapter transfers claims as data and operations through their
	// explicit codec. It never serializes a common Go AST or executable closure.
	claimData, err := json.Marshal(result.Rewrites)
	if err != nil {
		return err
	}
	var claims []plangraph.Rewrite
	if err := json.Unmarshal(claimData, &claims); err != nil {
		return err
	}
	contribution := result.Contributions[0]
	operationData, err := runtime.Codecs().Marshal(ctx, schemaext.Operation, []schemaext.Payload{contribution.Steps[0].Payload.Payload})
	if err != nil {
		return err
	}
	operations, err := runtime.Codecs().Unmarshal(ctx, operationData)
	if err != nil {
		return err
	}
	contribution.Steps[0].Payload.Payload = operations[0].(ast.ExtensionPayload)
	host := plangraph.Contribution[featureplan.Operation]{Owner: common.ID.Owner, Steps: []plangraph.Step[featureplan.Operation]{{ID: common.ID, Effects: common.Effects}}}
	plan, err := plangraph.ScheduleRewritten(ctx, host, claims, contribution)
	if err != nil {
		return err
	}
	if len(plan.Steps) != 1 || plan.Steps[0].ID != claims[0].Replacement || plan.Steps[0].Transaction != plangraph.TransactionForbidden || plan.Steps[0].Impact.Impact != schemaext.Behavioral {
		return fmt.Errorf("rewrite scheduling lost replacement identity or execution metadata")
	}
	rendered, err := runtime.Render(ctx, renderer.Request{Target: "widget", Nodes: []ast.Node{&ast.ExtensionStatement{Payload: plan.Steps[0].Payload.Payload}}})
	if err != nil {
		return err
	}
	if rendered.SQL() != "CREATE WIDGET extra\n" {
		return fmt.Errorf("rewritten operation lost the accepted common operand: %q", rendered.SQL())
	}
	return nil
}

func verifyPlanningRefusal(runtime *engine.Runtime) error {
	semantics := identifier.ForDialect("widget")
	request := featureplan.Request{Target: "widget", Identifiers: semantics, Tables: []featureplan.Table{{
		Action: featureplan.DropTable, Subject: objectidentity.NewBuilder(semantics).TableParts("", "items"),
		Current: schemacapture.TableObservation{Table: catalog.Table{Name: "items"}},
	}}}
	result, err := runtime.PlanFeatures(context.Background(), request)
	if err != nil {
		return err
	}
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	var transported featureplan.Result
	if err := json.Unmarshal(data, &transported); err != nil {
		return err
	}
	refused, ok := errors.AsType[*featureplan.RefusalError](transported.Err(request))
	if !ok || !errors.Is(refused, ptaherr.ErrUnsupportedFeature) || len(transported.Diagnostics) != 1 ||
		transported.Diagnostics[0].Parent == nil || *transported.Diagnostics[0].Parent != 0 || len(transported.Contributions) != 0 || len(transported.Parents) != 0 {
		return fmt.Errorf("planning refusal lost provenance or exposed output: %+v", transported)
	}
	return nil
}

func verifyBatchRefusal(runtime *engine.Runtime) error {
	node := ast.NewRawSQL("unsupported")
	result, err := runtime.Render(context.Background(), renderer.Request{Target: "widget", Nodes: []ast.Node{
		&ast.ExtensionStatement{Payload: &addWidget{Name: "prefix"}}, node,
	}})
	refused, ok := errors.AsType[*renderer.BatchRefusalError](err)
	if !ok || !errors.Is(err, ptaherr.ErrUnsupportedFeature) || len(refused.Diagnostics) != 1 ||
		refused.Diagnostics[0].Input == nil || *refused.Diagnostics[0].Input != 1 || result.Complete || len(result.Fragments) != 0 {
		return fmt.Errorf("completed batch refusal lost its data or exposed output: %+v, %v", result, err)
	}
	rendering, ok := errors.AsType[*ptaherr.RenderError](err)
	if !ok || rendering.Node != node {
		return fmt.Errorf("completed batch refusal lost caller input provenance: %v", err)
	}
	result, err = runtime.Render(context.Background(), renderer.Request{Target: "widget"})
	if err != nil || !result.Complete || len(result.Fragments) != 0 {
		return fmt.Errorf("empty batch was not completed: %+v, %v", result, err)
	}
	return nil
}

// verifyAccessEffects checks that an owner's access assessment survives the
// selected operation codec, and that an operation declaring one cannot be
// encoded without it.
func verifyAccessEffects(runtime *engine.Runtime) error {
	ctx := context.Background()
	grant := &grantWidget{Role: "reader", Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "the reader role gains every widget"}}
	data, err := runtime.Codecs().Marshal(ctx, schemaext.Operation, []schemaext.Payload{grant})
	if err != nil {
		return err
	}
	decoded, err := runtime.Codecs().Unmarshal(ctx, data)
	if err != nil {
		return err
	}
	source, ok := decoded[0].(schemaext.AccessEffectSource)
	if len(decoded) != 1 || !ok || source.AccessEffect() != grant.Access {
		return fmt.Errorf("operation codec lost the access assessment: %+v", decoded)
	}
	_, err = runtime.Codecs().Marshal(ctx, schemaext.Operation, []schemaext.Payload{&grantWidget{Role: "reader"}})
	if !errors.Is(err, schemaext.ErrInvalidValue) {
		return fmt.Errorf("an operation without its access assessment was encoded: %v", err)
	}
	return nil
}

type importService struct{ omit bool }

func (s importService) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	result := renderer.Result{Complete: true}
	for _, node := range request.Nodes {
		if err := ctx.Err(); err != nil {
			return renderer.Result{}, err
		}
		switch node := node.(type) {
		case *ast.CreateTableNode:
			result.Fragments = append(result.Fragments, "CREATE WIDGET TABLE "+node.Name+";")
		case *ast.DropTableNode:
			result.Fragments = append(result.Fragments, "DROP WIDGET TABLE "+node.Name+";")
		default:
			return renderer.Result{}, fmt.Errorf("unsupported import node %T", node)
		}
	}
	if s.omit {
		result.Omissions = []renderer.Omission{{Dialect: request.Target, Kind: "table", Name: "items", Reason: "unsupported", Property: "storage"}}
	}
	return result, nil
}

func verifyImport() error {
	ctx := context.Background()
	parser, err := importer.ParserByName("liquibase")
	if err != nil {
		return err
	}
	source := fstest.MapFS{"changelog.xml": {Data: []byte(`<databaseChangeLog><changeSet id="1" author="test"><createTable tableName="items"><column name="id" type="int"/></createTable></changeSet></databaseChangeLog>`)}}
	for _, omit := range []bool{false, true} {
		runtime, err := engine.New(engine.Provider{ID: "example.org/import", Targets: []engine.Target{{Name: "custom", Rendering: importService{omit: omit}}}})
		if err != nil {
			return err
		}
		configured, err := importer.WithRendering(parser, "custom", nil, runtime)
		if err != nil {
			return err
		}
		parsed, err := configured.Parse(ctx, source)
		if omit {
			if err == nil || parsed != nil || !strings.Contains(err.Error(), "cannot carry the whole change") {
				return fmt.Errorf("selected importer lost an omission: %v, %v", parsed, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if len(parsed.Migrations) != 1 || parsed.Migrations[0].UpSQL != "CREATE WIDGET TABLE items;" || parsed.Migrations[0].DownSQL != "DROP WIDGET TABLE items;" {
			return fmt.Errorf("selected importer lost provider SQL: %+v", parsed)
		}
	}
	return nil
}

func (service) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	result := schemavalidation.Result{Complete: true}
	for _, facets := range request.Schema.FacetSlots() {
		if err := ctx.Err(); err != nil {
			return schemavalidation.Result{}, err
		}
		value, found, err := schemaext.FacetAs[*widget](*facets, (&widget{}).Kind())
		if err != nil {
			return schemavalidation.Result{}, err
		}
		if !found {
			continue
		}
		if strings.TrimSpace(value.Name) == "" || len(value.Levels) == 0 {
			result.Diagnostics = append(result.Diagnostics, schemavalidation.Diagnostic{
				Code: schemavalidation.InvalidSchema, Kind: "widget", Object: value.Name,
				Message: "a widget requires a name and at least one level",
			})
		}
	}
	return result, nil
}

func verifyValidation(runtime *engine.Runtime, decoded schemaext.Facets) error {
	ctx := context.Background()
	request := schemavalidation.Request{Target: "widget", Schema: &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "first", Facets: decoded}, {Name: "second", Facets: decoded}},
	}}
	result, err := runtime.ValidateSchema(ctx, request)
	if err != nil {
		return err
	}
	if err := result.Err("widget"); err != nil {
		return err
	}
	invalid, err := schemaext.NewFacets(&widget{Name: "invalid"})
	if err != nil {
		return err
	}
	request.Schema.Tables[1].Facets = invalid
	result, err = runtime.ValidateSchema(ctx, request)
	if err != nil {
		return err
	}
	if !errors.Is(result.Err("widget"), ptaherr.ErrInvalidSchemaDiff) || len(result.Diagnostics) != 1 || result.Diagnostics[0].Object != "invalid" {
		return fmt.Errorf("whole-schema validation lost the second declaration's refusal: %+v", result)
	}
	request.Target = "postgres"
	_, err = runtime.ValidateSchema(ctx, request)
	if !errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		return fmt.Errorf("unselected validation target was not refused: %v", err)
	}
	return nil
}

func (s service) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	validation, err := s.ValidateSchema(ctx, schemavalidation.Request{
		Target: request.Target, Schema: request.Schema, Capabilities: request.Capabilities, Identifiers: request.Identifiers,
	})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	result := renderer.SchemaResult{Complete: true, Diagnostics: validation.Diagnostics}
	if len(result.Diagnostics) != 0 {
		return result, nil
	}
	for _, table := range request.Schema.Tables {
		value, found, err := schemaext.FacetAs[*widget](table.Facets, (&widget{}).Kind())
		if err != nil {
			return renderer.SchemaResult{}, err
		}
		if found {
			result.Statements = append(result.Statements, fmt.Sprintf("CREATE WIDGET %q.%q;\n", table.Name, value.Name))
		}
	}
	return result, nil
}

func verifySchemaRendering(runtime *engine.Runtime, decoded schemaext.Facets) error {
	request := renderer.SchemaRequest{Target: "widget", Schema: &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "first", Facets: decoded}, {Name: "second", Facets: decoded}},
	}}
	result, err := runtime.RenderSchema(context.Background(), request)
	if err != nil {
		return err
	}
	if !slices.Equal(result.Statements, []string{"CREATE WIDGET \"first\".\"sample\";\n", "CREATE WIDGET \"second\".\"sample\";\n"}) {
		return fmt.Errorf("whole-schema rendering lost declaration order: %+v", result)
	}
	invalid, err := schemaext.NewFacets(&widget{Name: "invalid"})
	if err != nil {
		return err
	}
	request.Schema.Tables[1].Facets = invalid
	result, err = runtime.RenderSchema(context.Background(), request)
	if !errors.Is(err, ptaherr.ErrInvalidSchemaDiff) || len(result.Statements) != 0 {
		return fmt.Errorf("refused rendering exposed SQL: %+v, %v", result, err)
	}
	request.Target = "postgres"
	_, err = runtime.RenderSchema(context.Background(), request)
	if !errors.Is(err, ptaherr.ErrUnsupportedDialect) {
		return fmt.Errorf("unselected schema renderer was not refused: %v", err)
	}
	return nil
}

type scopedService struct{}

func (scopedService) ValidateSchema(_ context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	if request.Target != "custom" || len(request.Schema.CompositeTypes) != 1 || request.Schema.CompositeTypes[0].Name != "local" {
		return schemavalidation.Result{}, fmt.Errorf("custom scope was not applied before dispatch")
	}
	return schemavalidation.Result{Complete: true}, nil
}

func (s scopedService) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	_, err := s.ValidateSchema(ctx, schemavalidation.Request{Target: request.Target, Schema: request.Schema})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	return renderer.SchemaResult{Complete: true, Statements: []string{"CREATE TYPE local;"}}, nil
}

func verifyTargetScope(foreign schemaext.Facets) error {
	// No codecs are registered here: the excluded declaration belongs to a
	// different target and must be removed before its payload is inspected.
	runtime, err := engine.New(engine.Provider{ID: "example.org/scoped", Targets: []engine.Target{{
		Name: "custom", Aliases: []string{"custom+wire"}, Validation: scopedService{}, SchemaRendering: scopedService{},
	}}})
	if err != nil {
		return err
	}
	schema := &schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{
		{Name: "foreign", Dialects: []string{"postgres"}, Facets: foreign},
		{Name: "local", Dialects: []string{" CUSTOM+WIRE "}},
	}}
	ctx := context.Background()
	validation := schemavalidation.Request{Target: "custom+wire", Schema: schema}
	if _, err := runtime.ValidateSchema(ctx, validation); err != nil {
		return err
	}
	rendering := renderer.SchemaRequest{Target: "custom+wire", Schema: schema}
	result, err := runtime.RenderSchema(ctx, rendering)
	if err != nil {
		return err
	}
	if !slices.Equal(result.Statements, []string{"CREATE TYPE local;"}) || len(schema.CompositeTypes) != 2 {
		return fmt.Errorf("custom target scope changed source data or lost selected output")
	}
	schema.CompositeTypes[0].Dialects = nil
	_, validationErr := runtime.ValidateSchema(ctx, validation)
	_, renderingErr := runtime.RenderSchema(ctx, rendering)
	if !errors.Is(validationErr, schemaext.ErrUnknownCodec) || !errors.Is(renderingErr, schemaext.ErrUnknownCodec) {
		return fmt.Errorf("unscoped unknown payload was not refused: %v, %v", validationErr, renderingErr)
	}
	return verifyFacetTargetScope(runtime, foreign)
}

func verifyFacetTargetScope(runtime *engine.Runtime, foreign schemaext.Facets) error {
	source, err := foreign.WithTargetScope((&widget{}).Kind(), "postgres")
	if err != nil {
		return err
	}
	target, err := runtime.ResolveTarget("custom+wire")
	if err != nil {
		return err
	}
	selected, err := source.ForTarget(target)
	if err != nil {
		return err
	}
	ctx := context.Background()
	encoded, err := runtime.Codecs().EncodeFacets(ctx, schemaext.Desired, selected)
	if err != nil {
		return err
	}
	decoded, err := runtime.Codecs().DecodeFacets(ctx, schemaext.Desired, encoded)
	if err != nil {
		return err
	}
	if decoded.Len() != 0 || decoded.IsZero() || !slices.Equal(decoded.TargetScope((&widget{}).Kind()), []string{"postgres"}) {
		return fmt.Errorf("facet source binding was lost in an excluded capture")
	}
	schema := &schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{{Name: "local", Facets: decoded}}}
	_, err = runtime.RenderSchema(ctx, renderer.SchemaRequest{Target: "custom+wire", Schema: schema})
	return err
}

func (service) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	result := schemaprojection.TableCreationResult{Complete: true}
	for _, table := range request.Tables {
		if err := ctx.Err(); err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		values, err := schemaext.NewFacets(&widget{Name: table.Declaration.Table.Name, Levels: []string{"default"}})
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		result.Tables = append(result.Tables, schemaprojection.TableCreation{Subject: table.Subject, Facets: []schemaext.FacetRecord{{Subject: table.Subject, Values: values}}})
	}
	return result, nil
}

func verifyCreationProjection(runtime *engine.Runtime) error {
	semantics := identifier.ForDialect("widget")
	subject := objectidentity.NewBuilder(semantics).TableParts("", "sample")
	request := schemaprojection.TableCreationRequest{Target: "widget", Identifiers: semantics,
		Tables: []schemaprojection.TableCreationInput{{Subject: subject, Declaration: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "sample"}}}},
	}
	result, err := runtime.ProjectTableCreations(context.Background(), request)
	if err != nil {
		return err
	}
	if !result.Complete || len(result.Tables) != 1 {
		return fmt.Errorf("creation prediction did not complete")
	}
	value, found, err := schemaext.FacetAs[*widget](result.Tables[0].Facets[0].Values, (&widget{}).Kind())
	if err != nil {
		return err
	}
	if !found || value.Name != "sample" || !slices.Equal(value.Levels, []string{"default"}) {
		return fmt.Errorf("creation prediction lost provider defaults")
	}
	if !request.Tables[0].Declaration.Table.Facets.IsZero() {
		return fmt.Errorf("creation prediction changed source intent")
	}
	return nil
}
