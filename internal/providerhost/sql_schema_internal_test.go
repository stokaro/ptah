package providerhost

// White-box testing required: the mapping from a provider's SQL schema reading
// to the host model is private to the adapter, and a unit test cannot start a
// provider process. The field audit must see every collection of a reading
// reach the model without one.

import (
	"errors"
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/providerdiagnostic"
)

// sqlSchemaFieldHomes is the field audit ADR 0027 asks for: every field of
// schemamodel.Database, and where a provider's SQL schema reading puts it. A
// field a reading's common objects fill names its collection in SQLSchema. The
// others say what fills them instead, or why nothing does: the provider
// reports a construct with no home as UNSUPPORTED and names it.
var sqlSchemaFieldHomes = map[string]struct {
	home   string
	common bool
}{
	"Schemas":                    {"namespaces", true},
	"Tables":                     {"tables", true},
	"Fields":                     {"tables.columns", true},
	"Indexes":                    {"tables.indexes", true},
	"Constraints":                {"tables.constraints", true},
	"Enums":                      {"enums and tables.enums", true},
	"Sequences":                  {"sequences", true},
	"Views":                      {"views", true},
	"MaterializedViews":          {"views with materialized set", true},
	"Triggers":                   {"tables.triggers", true},
	"Roles":                      {"roles", true},
	"Grants":                     {"grants", true},
	"RevokedGrants":              {"grants with revoked set", true},
	"FeatureObjects":             {"native_objects, normalized by the provider", false},
	"FeatureCoverage":            {"coverage, the claim of the same reading", false},
	"NotDescribed":               {"filled by the reader from each document's header, not by the reply", false},
	"Dependencies":               {"derived from the tables by schemamodel.Finalize", false},
	"SelfReferencingForeignKeys": {"derived from the tables by schemamodel.Finalize", false},
	"FunctionDependencies":       {"derived by schemamodel.Finalize; functions have no home", false},
	"Functions":                  {"refused: the provider reports UNSUPPORTED", false},
	"RLSPolicies":                {"refused: the provider reports UNSUPPORTED", false},
	"RLSEnabledTables":           {"refused: the provider reports UNSUPPORTED", false},
	"ManagedData":                {"refused: the provider reports UNSUPPORTED", false},
	"EmbeddedFields":             {"refused: Go struct embedding has no SQL spelling", false},
	"EmbeddedSources":            {"refused: Go struct embedding has no SQL spelling", false},
	"Facets":                     {"refused: a database setting is a native object of the provider", false},
}

// fullSQLSchemaReply fills every collection of SQLSchema whose common objects
// the host maps.
func fullSQLSchemaReply() *protocol.SQLSchema {
	return &protocol.SQLSchema{
		Complete: true,
		Tables: []*protocol.DeclaredTable{{
			Name: "items", Schema: "app", PrimaryKey: []string{"id"}, PrimaryKeyParts: []*protocol.DeclaredKeyPart{{Column: "id", Descending: true}},
			Columns:     []*protocol.DeclaredColumn{{Name: "id", Type: "Int64"}, {Name: "state", Type: "state", EnumValues: []string{"on", "off"}, Nullable: true}},
			Enums:       []*protocol.DeclaredEnum{{Name: "state", Schema: "app", Values: []string{"on", "off"}}},
			Constraints: []*protocol.DeclaredConstraint{{Name: "positive", Type: "CHECK", CheckExpression: "id > 0"}},
			Indexes:     []*protocol.DeclaredIndex{{Name: "by_state", Fields: []string{"state"}, Parts: []*protocol.CapturedIndexPart{{Column: "state"}}}},
			Triggers:    []*protocol.CapturedTrigger{{Name: "touch", Timing: "AFTER", Event: "INSERT", Body: "SELECT 1"}},
		}},
		Namespaces: []*protocol.DeclaredNamespace{{Name: "app", Comment: "application"}},
		Enums:      []*protocol.DeclaredEnum{{Name: "state", Schema: "app", Values: []string{"on", "off"}}},
		Views:      []*protocol.SQLView{{Schema: "app", Name: "visible", Body: "SELECT id FROM app.items"}, {Schema: "app", Name: "counted", Body: "SELECT count(*) FROM app.items", Materialized: true}},
		Sequences:  []*protocol.SQLSequence{{Schema: "app", Name: "ids", Increment: new(int64(2))}},
		Roles:      []*protocol.SQLRole{{Name: "reader", Login: true}},
		Grants: []*protocol.SQLGrant{
			{Role: "reader", Privileges: []string{"SELECT"}, Target: &protocol.Reference{Kind: "table", Schema: "app", Name: "items"}},
			{Role: "reader", Privileges: []string{"INSERT"}, Target: &protocol.Reference{Kind: "table", Schema: "app", Name: "items"}, Revoked: true},
		},
	}
}

// TestSQLSchemaFieldAudit fails when schemamodel.Database gains a field this
// audit does not place, and when a field the audit says a reading's common
// objects fill stays empty after mapping a reading that holds every one of
// them, or a field it says they leave alone is filled.
func TestSQLSchemaFieldAudit(t *testing.T) {
	c := qt.New(t)
	database, err := sqlSchemaCommon(fullSQLSchemaReply())
	c.Assert(err, qt.IsNil)
	reflected := reflect.ValueOf(database)
	var fields []string
	for i := range reflected.NumField() {
		name := reflected.Type().Field(i).Name
		fields = append(fields, name)
		entry, placed := sqlSchemaFieldHomes[name]
		c.Assert(placed, qt.IsTrue, qt.Commentf("schemamodel.Database.%s has no home in a SQL schema reading and no named refusal", name))
		c.Assert(!reflected.Field(i).IsZero(), qt.Equals, entry.common, qt.Commentf("schemamodel.Database.%s: %s", name, entry.home))
	}
	for name := range sqlSchemaFieldHomes {
		c.Assert(fields, qt.Contains, name, qt.Commentf("the audit places %s, which schemamodel.Database does not have", name))
	}
}

func TestSQLSchemaCommonMapsDeclaredObjects(t *testing.T) {
	c := qt.New(t)
	database, err := sqlSchemaCommon(fullSQLSchemaReply())
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.DeepEquals, []schemamodel.Table{{StructName: "app.items", Name: "items", Schema: "app", PrimaryKey: []string{"id"},
		PrimaryKeyParts: []schemamodel.PrimaryKeyPart{{Name: "id", Desc: true}}}})
	c.Assert(database.Fields[1], qt.DeepEquals, schemamodel.Field{StructName: "app.items", Name: "state", Type: "state", Enum: []string{"on", "off"}, Nullable: true})
	c.Assert(database.Enums, qt.HasLen, 1)
	c.Assert(database.Constraints[0].Table, qt.Equals, "app.items")
	c.Assert(database.Indexes[0].TableName, qt.Equals, "app.items")
	c.Assert(database.Triggers[0].Table, qt.Equals, "app.items")
	c.Assert(database.Views[0].Name, qt.Equals, "app.visible")
	c.Assert(database.MaterializedViews[0].Name, qt.Equals, "app.counted")
	c.Assert(*database.Sequences[0].Increment, qt.Equals, int64(2))
	c.Assert(database.Grants, qt.DeepEquals, []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "app.items"}})
	c.Assert(database.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "app.items"}})
}

func TestSQLSchemaGrantTargets(t *testing.T) {
	for _, test := range []struct {
		name   string
		target *protocol.Reference
		want   schemamodel.Grant
	}{
		{"schema", &protocol.Reference{Kind: "schema", Name: "app"}, schemamodel.Grant{OnSchema: "app"}},
		{"sequence", &protocol.Reference{Kind: "sequence", Schema: "app", Name: "ids"}, schemamodel.Grant{OnSequence: "app.ids"}},
		{"database", &protocol.Reference{Kind: "database", Name: "local"}, schemamodel.Grant{OnDatabase: true}},
		{"routine", &protocol.Reference{Kind: "function", Schema: "app", Name: "f", Overload: []string{"integer", "text"}},
			schemamodel.Grant{OnRoutine: "app.f", RoutineArguments: "integer, text", RoutineKind: "FUNCTION"}},
		{"scheme object", &protocol.Reference{Kind: "topic", Schema: "app", Name: "events"}, schemamodel.Grant{OnObject: "app.events", ObjectKind: schemamodel.GrantObjectTopic}},
		{"scheme object of two words", &protocol.Reference{Kind: "coordination node", Schema: "app", Name: "locks"},
			schemamodel.Grant{OnObject: "app.locks", ObjectKind: schemamodel.GrantObjectCoordinationNode}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grant, err := sqlGrant(&protocol.SQLGrant{Role: "reader", Privileges: []string{"USE"}, Target: test.target})
			c.Assert(err, qt.IsNil)
			test.want.Role, test.want.Privileges = "reader", []string{"USE"}
			c.Assert(grant, qt.DeepEquals, test.want)
		})
	}
}

func TestSQLSchemaRefusesWhatTheModelCannotHold(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*protocol.SQLSchema)
	}{
		{"table facets", func(s *protocol.SQLSchema) {
			s.Tables[0].Features = &protocol.CapturedFeatures{Facets: &protocol.ComparisonState{Representation: protocol.ValueInput_DESIRED}}
		}},
		{"trigger in another schema", func(s *protocol.SQLSchema) { s.Tables[0].Triggers[0].Schema = "other" }},
		{"materialized view with check option", func(s *protocol.SQLSchema) { s.Views[1].WithCheck = true }},
		{"grant on an unknown kind", func(s *protocol.SQLSchema) { s.Grants[0].Target.Kind = "gadget" }},
		{"grant on a qualified path", func(s *protocol.SQLSchema) { s.Grants[0].Target.Path = []string{"dir"} }},
		{"argument types of a table", func(s *protocol.SQLSchema) { s.Grants[0].Target.Overload = []string{"integer"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reply := fullSQLSchemaReply()
			test.change(reply)
			database, err := sqlSchemaCommon(reply)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

func TestSQLSchemaRefusesAnEnumDeclaredTwiceDifferently(t *testing.T) {
	c := qt.New(t)
	reply := fullSQLSchemaReply()
	reply.Enums[0].Values = []string{"on"}
	database, err := sqlSchemaCommon(reply)
	c.Assert(err, qt.ErrorMatches, `.*declares enum "app.state" twice, differently`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}

func sqlRefusalRequest() *protocol.ReadSQLSchemaRequest {
	return &protocol.ReadSQLSchemaRequest{Documents: []*protocol.SQLDocument{{Id: "1", Path: "one.sql"}, {Id: "2", Path: "two.sql"}}}
}

func sqlDiagnostic(code protocol.SQLDiagnostic_Code, severity protocol.SQLDiagnostic_Severity, document string, offset uint64, message string) *protocol.SQLDiagnostic {
	return &protocol.SQLDiagnostic{Code: code, Severity: severity, DocumentId: document, Message: message, Span: &protocol.SourceSpan{
		Start: &protocol.SourcePosition{ByteOffset: offset, Line: 2, Column: 3}, End: &protocol.SourcePosition{ByteOffset: offset + 4, Line: 2, Column: 7},
	}}
}

// TestSQLSchemaRefusalNamesTheFirstError prints the first error, not the first
// diagnostic, in the shared parser's layout under its document's path, and
// keeps every diagnostic for a JSON report.
func TestSQLSchemaRefusalNamesTheFirstError(t *testing.T) {
	c := qt.New(t)
	reply := &protocol.SQLSchema{Diagnostics: []*protocol.SQLDiagnostic{
		sqlDiagnostic(protocol.SQLDiagnostic_INVALID, protocol.SQLDiagnostic_WARNING, "1", 5, "first warning"),
		sqlDiagnostic(protocol.SQLDiagnostic_UNSUPPORTED, protocol.SQLDiagnostic_ERROR, "2", 9, "unsupported statement GRANT"),
		sqlDiagnostic(protocol.SQLDiagnostic_SYNTAX, protocol.SQLDiagnostic_ERROR, "1", 1, "second error"),
	}}

	err := sqlSchemaRefusal(sqlRefusalRequest(), reply)

	c.Assert(err, qt.ErrorMatches, `two.sql: unsupported statement GRANT at position 9 \(line 2, column 3\)`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	refusal, ok := errors.AsType[*providerdiagnostic.SchemaRefusal](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(refusal.Diagnostics, qt.HasLen, 3)
	c.Assert(refusal.Diagnostics[0], qt.DeepEquals, providerdiagnostic.SchemaDiagnostic{Code: "invalid", Severity: "warning", Path: "one.sql", Message: "first warning",
		Start: providerdiagnostic.SchemaPosition{Offset: 5, Line: 2, Column: 3}, End: providerdiagnostic.SchemaPosition{Offset: 9, Line: 2, Column: 7}})
}

func TestSQLSchemaRefusalOfASyntaxErrorIsNotAnUnsupportedFeature(t *testing.T) {
	c := qt.New(t)
	reply := &protocol.SQLSchema{Diagnostics: []*protocol.SQLDiagnostic{sqlDiagnostic(protocol.SQLDiagnostic_SYNTAX, protocol.SQLDiagnostic_ERROR, "1", 1, "expected a name")}}

	err := sqlSchemaRefusal(sqlRefusalRequest(), reply)

	c.Assert(err, qt.ErrorMatches, `one.sql: expected a name at position 1 \(line 2, column 3\)`)
	c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrUnsupportedFeature)
}

func TestSQLSchemaRefusesAReadingWithWarnings(t *testing.T) {
	c := qt.New(t)
	reply := &protocol.SQLSchema{Complete: true, Diagnostics: []*protocol.SQLDiagnostic{sqlDiagnostic(protocol.SQLDiagnostic_INVALID, protocol.SQLDiagnostic_WARNING, "2", 4, "deprecated")}}

	err := sqlSchemaRefusal(sqlRefusalRequest(), reply)

	c.Assert(err, qt.ErrorMatches, `two.sql: the provider reported a warning, which Ptah does not report for a SQL schema reading yet; the first: deprecated at position 4 \(line 2, column 3\)`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}

func sqlDocumentTarget() *target {
	engine := &protocol.Engine{Id: "fixture", DefaultSchema: "main", Features: []string{provider.ReadSQLSchemaFeature},
		Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
		SqlSchema:   &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_DOCUMENT, HeaderKinds: []string{"topic"}}}
	description := &protocol.Description{ProviderId: "example.org/fixture", Engines: []*protocol.Engine{engine}}
	host := &Host{description: description, targets: make(map[string]*target)}
	host.targets["fixture"] = &target{host: host, engine: engine}
	return host.targets["fixture"]
}

// TestSQLSchemaRequestSendsTheEnginesHeaderEntries sends the documents in
// order, names a document without a path by its place, and sends the header
// entries of the engine's kinds alone. The reader decodes the header again for
// the common kinds and refuses a kind nobody reads.
func TestSQLSchemaRequestSendsTheEnginesHeaderEntries(t *testing.T) {
	c := qt.New(t)
	request, err := sqlDocumentTarget().sqlSchemaRequest(sqlext.GrammarRequest{Target: "fixture", DatabasePath: "/local", Files: []sqlext.SourceFile{
		{Path: "a.sql", Text: "-- ptah:not-described TOPIC \"events\"\n-- ptah:not-described view\n-- ptah:not-described changefeed\nCREATE TOPIC x;"},
		{Text: "-- ptah:not-described topic\n"},
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(request.GetDatabasePath(), qt.Equals, "/local")
	c.Assert(request.GetDocuments(), qt.HasLen, 2)
	c.Assert(request.GetDocuments()[1].GetPath(), qt.Equals, "document 2")
	c.Assert(request.GetIdentifiers().GetDefaultSchema(), qt.Equals, "main")
	c.Assert(request.GetLimits(), qt.HasLen, 2)
	c.Assert(proto.Equal(request.GetLimits()[0], &protocol.SchemaCoverageLimit{Kind: "topic", Name: "events"}), qt.IsTrue)
	c.Assert(proto.Equal(request.GetLimits()[1], &protocol.SchemaCoverageLimit{Kind: "topic"}), qt.IsTrue)
	_, err = sqlDocumentTarget().sqlSchemaRequest(sqlext.GrammarRequest{Target: "other"})
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}

func TestSQLHeaderKindsAreTheProvidersOwn(t *testing.T) {
	for _, test := range []struct {
		name string
		kind string
	}{
		{"common kind", "view"},
		{"two words", "topic name"},
		{"quoted", `topic"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := sqlDocumentTarget().host.description
			description.Engines[0].SqlSchema.HeaderKinds = []string{test.kind}
			c.Assert(checkSQLHeaderKinds(description), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	c.Assert(checkSQLHeaderKinds(sqlDocumentTarget().host.description), qt.IsNil)
}

func TestSQLExtensionsServeDocumentEnginesAlone(t *testing.T) {
	c := qt.New(t)
	host := sqlDocumentTarget().host
	fragments := &protocol.Engine{Id: "fragments"}
	host.description.Engines = append(host.description.Engines, fragments)
	host.targets["fragments"] = &target{host: host, engine: fragments}
	extensions := host.sqlExtensions()
	c.Assert(extensions, qt.HasLen, 1)
	c.Assert(extensions[0].Owner, qt.Equals, "example.org/fixture")
	c.Assert(extensions[0].Targets, qt.DeepEquals, []string{"fixture"})
	c.Assert(extensions[0].Grammar, qt.IsNotNil)
	c.Assert(extensions[0].Header, qt.HasLen, 1)
	c.Assert(string(extensions[0].Header[0]), qt.Equals, "topic")
}

func TestContractIdentityBindsHowSQLDocumentsAreRead(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*protocol.Engine)
	}{
		{"mode", func(engine *protocol.Engine) { engine.SqlSchema.Mode = protocol.SQLSchemaDeclaration_FRAGMENTS }},
		{"header kinds", func(engine *protocol.Engine) { engine.SqlSchema.HeaderKinds = []string{"secret"} }},
		{"declaration", func(engine *protocol.Engine) { engine.SqlSchema = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := modelIdentityTarget()
			target.engine.SqlSchema = &protocol.SQLSchemaDeclaration{Mode: protocol.SQLSchemaDeclaration_DOCUMENT, HeaderKinds: []string{"topic"}}
			before, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			test.change(target.engine)
			after, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			c.Assert(after.ContractSHA256, qt.Not(qt.Equals), before.ContractSHA256)
		})
	}
}

// sqlCoverageTarget registers one desired model of the fixture engine, written
// in SQL there and not in SQL on each engine of withoutSQL.
func sqlCoverageTarget(withoutSQL ...string) *target {
	selected := sqlDocumentTarget()
	model := &protocol.ValueModel{Kind: "example.org/fixture/topic", Representation: protocol.ValueInput_DESIRED, SourceFormats: []protocol.SourceDocument_Format{protocol.SourceDocument_SQL}}
	contract := &modelContract{key: modelKey{kind: "example.org/fixture/topic", representation: schemaext.Desired}, engines: map[string]*protocol.ValueModel{"fixture": model}, signature: "sha256:topic"}
	for _, engine := range withoutSQL {
		contract.engines[engine] = &protocol.ValueModel{Kind: model.Kind, Representation: model.Representation}
	}
	selected.host.models = map[modelKey]*modelContract{contract.key: contract}
	return selected
}

func sqlCoverageReply(state protocol.ModelKnowledge_State) *protocol.ModelCoverage {
	return &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED,
		Kinds: []*protocol.ModelKindCoverage{{Model: &protocol.ValueModel{Kind: "example.org/fixture/topic"}, Knowledge: &protocol.ModelKnowledge{State: state, Reason: "the header leaves it undescribed"}}},
		Subjects: []*protocol.ModelSubjectCoverage{{Kind: "example.org/fixture/topic", Subject: &protocol.Reference{Kind: "example.org/fixture/topic", Name: "events"},
			NormalizedSubject: &protocol.Reference{Kind: "example.org/fixture/topic", Name: "events"}, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "kept"}}}}
}

// TestSQLCoverageKeepsWhatTheReadingStates keeps the knowledge the provider
// states for a kind and a subject, an undescribed one included, where the
// claim of other source formats would read a kind as complete.
func TestSQLCoverageKeepsWhatTheReadingStates(t *testing.T) {
	for _, test := range []struct {
		name       string
		state      protocol.ModelKnowledge_State
		withoutSQL []string
		want       schemaext.KnowledgeState
	}{
		{"complete", protocol.ModelKnowledge_COMPLETE, nil, schemaext.Complete},
		{"undescribed by the header", protocol.ModelKnowledge_UNINSPECTED, nil, schemaext.Uninspected},
		{"not SQL on another engine", protocol.ModelKnowledge_COMPLETE, []string{"other"}, schemaext.Uninspected},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			claim, err := sqlCoverageTarget(test.withoutSQL...).sqlCoverage(sqlCoverageReply(test.state))
			c.Assert(err, qt.IsNil)
			c.Assert(claim.Representation(), qt.Equals, schemaext.Desired)
			c.Assert(claim.KindRecords(), qt.HasLen, 1)
			c.Assert(claim.Lookup("example.org/fixture/topic", objectidentity.ID{Kind: "example.org/fixture/topic", Name: objectidentity.Part{Source: "other", Normalized: "other"}}).State, qt.Equals, test.want)
			c.Assert(claim.SubjectRecords(), qt.HasLen, 1)
			c.Assert(claim.SubjectRecords()[0].Knowledge, qt.DeepEquals, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "kept"})
		})
	}
}

func TestSQLCoverageRefusesNamespaceRegions(t *testing.T) {
	c := qt.New(t)
	reply := sqlCoverageReply(protocol.ModelKnowledge_COMPLETE)
	reply.Kinds[0].Regions = []*protocol.ModelNamespaceKnowledge{{}}
	claim, err := sqlCoverageTarget().sqlCoverage(reply)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(claim.Representation(), qt.Equals, schemaext.Representation(""))
}
