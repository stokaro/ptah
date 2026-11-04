package providerhost

// White-box testing required: an encoding request is built, its regions
// spelled and its tables bound before the EncodeSource call, and only a
// provider process observes the request on the public path. The
// providercli e2e tests drive that path through the fixture provider; these
// cases spell every region shape and refusal without a process per shape.

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

const (
	// encodedJobKind is a model whose objects no scope selection names, so
	// its objects keep the database they were read in.
	encodedJobKind schemaext.Kind = "example.org/job"
	// encodedQueueKind is a model a scope selection names, so its objects
	// are held without their database.
	encodedQueueKind schemaext.Kind = "example.org/queue"
)

// regionHost is a provider with the engine "example", which encodes, selects
// queues and holds the models of jobs and queues, and the engine "other".
func regionHost(c *qt.C) *Host {
	c.Helper()
	description := scopeDescription()
	description.Features = append(description.Features, provider.EncodeSourceFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.EncodeSourceFeature)
	description.Engines = append(description.Engines, &protocol.Engine{Id: "other", DefaultSchema: "main", Identifiers: description.Engines[0].GetIdentifiers()})
	selections, err := newScopeSelections(description)
	c.Assert(err, qt.IsNil)
	host := &Host{description: description, selections: selections, targets: make(map[string]*target), models: make(map[modelKey]*modelContract)}
	for _, engine := range description.GetEngines() {
		host.targets[engine.GetId()] = &target{host: host, engine: engine}
	}
	for _, kind := range []schemaext.Kind{encodedJobKind, encodedQueueKind} {
		key := modelKey{kind: kind, representation: schemaext.Desired}
		host.models[key] = &modelContract{key: key, signature: "contract", engines: map[string]*protocol.ValueModel{
			"example": {Kind: string(kind), Representation: protocol.ValueInput_DESIRED},
		}}
	}
	return host
}

// readKnowledge is the knowledge the host holds after reading regions of kind
// in a session of engine on the database /opened.
func readKnowledge(c *qt.C, host *Host, engine string, kind schemaext.Kind, regions []*protocol.ModelNamespaceKnowledge) schemaext.KnowledgeDomain {
	c.Helper()
	r := reader{session: &session{target: host.targets[engine], remote: &protocol.Session{Engine: engine, Database: "/opened"},
		info: catalog.ServerInfo{IdentifierSemantics: regionSemantics(c, host)}}}
	domain, err := r.observedNamespaceKnowledge(kind, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside the listed directories"}, regions)
	c.Assert(err, qt.IsNil)
	return domain
}

func regionSemantics(c *qt.C, host *Host) identifier.Semantics {
	c.Helper()
	engine := host.targets["example"].engine
	semantics, err := identifierRules(engine.GetIdentifiers(), engine.GetDefaultSchema())
	c.Assert(err, qt.IsNil)
	return semantics
}

// documentCoverage is a document's knowledge: domains by model.
func documentCoverage(c *qt.C, host *Host, domains map[schemaext.Kind]schemaext.KnowledgeDomain) schemaext.Coverage {
	c.Helper()
	var kinds []schemaext.KindCoverage
	for kind, domain := range domains {
		kinds = append(kinds, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: host.description.GetProviderId(), Kind: kind,
			Representation: schemaext.Desired, Version: 1, Definition: "definition"}, Knowledge: domain})
	}
	known, err := schemaext.NewCoverage(schemaext.Desired, kinds, nil)
	c.Assert(err, qt.IsNil)
	return known
}

// encodedValue is a desired value of kind whose owner is owner.
func encodedValue(kind schemaext.Kind, owner *protocol.Reference) *resolvedModel {
	return &resolvedModel{kind: kind, signature: "contract", canonical: []byte(`{}`), wire: modelWire{Format: 1, Engine: "example"},
		normalized: &protocol.NormalizedValue{Value: &protocol.Envelope{Kind: string(kind), Owner: owner}, NormalizedOwner: owner}}
}

func namespace(database, inCatalog, schema string, path ...string) *protocol.ModelNamespace {
	scope := &protocol.ModelNamespace{Database: &protocol.NamespaceComponent{Source: database, Normalized: database},
		Catalog: &protocol.NamespaceComponent{Source: inCatalog, Normalized: inCatalog}, Schema: &protocol.NamespaceComponent{Source: schema, Normalized: schema}}
	for _, part := range path {
		scope.PathPrefix = append(scope.PathPrefix, &protocol.NamespaceComponent{Source: part, Normalized: part})
	}
	return scope
}

// databaseOnly is a namespace of a whole database.
func databaseOnly(database string) *protocol.ModelNamespace {
	return &protocol.ModelNamespace{Database: &protocol.NamespaceComponent{Source: database, Normalized: database}}
}

// withoutDatabase is scope matching every database.
func withoutDatabase(scope *protocol.ModelNamespace) *protocol.ModelNamespace {
	scope.Database = nil
	return scope
}

func complete(scope *protocol.ModelNamespace) *protocol.ModelNamespaceKnowledge {
	return &protocol.ModelNamespaceKnowledge{Scope: scope, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}
}

// regionTexts writes each region as a line, so a failing assertion prints
// it: quicktest formats a message by walking its descriptors, which does not
// finish. A component is written with whether it is present, since an absent
// one matches every name and a present empty one only an empty name.
func regionTexts(regions []*protocol.ModelNamespaceKnowledge) []string {
	component := func(part *protocol.NamespaceComponent) string {
		return fmt.Sprintf("%t:%q:%q", part != nil, part.GetSource(), part.GetNormalized())
	}
	var texts []string
	for _, region := range regions {
		scope := region.GetScope()
		path := make([]string, 0, len(scope.GetPathPrefix()))
		for _, part := range scope.GetPathPrefix() {
			path = append(path, component(part))
		}
		texts = append(texts, fmt.Sprintf("database %s catalog %s schema %s path %v: %s %q", component(scope.GetDatabase()), component(scope.GetCatalog()),
			component(scope.GetSchema()), path, region.GetKnowledge().GetState(), region.GetKnowledge().GetReason()))
	}
	return texts
}

// coveredKinds are the models a request's coverage states knowledge of, by
// kind and subject record, as regionTexts writes regions.
func coveredKinds(coverage *protocol.ModelCoverage) []string {
	var kinds []string
	for _, kind := range coverage.GetKinds() {
		kinds = append(kinds, "kind "+kind.GetModel().GetKind())
	}
	for _, subject := range coverage.GetSubjects() {
		kinds = append(kinds, "subject "+subject.GetKind())
	}
	return kinds
}

// valueIDs are the IDs of a request's values.
func valueIDs(values []*protocol.EncodeSourceValue) []string {
	var ids []string
	for _, value := range values {
		ids = append(ids, value.GetValue().GetId())
	}
	return ids
}

// bindingTexts writes each binding of a request as a line, as regionTexts
// writes regions.
func bindingTexts(bindings []*protocol.SourceBinding) []string {
	var texts []string
	for _, binding := range bindings {
		table := binding.GetObject()
		texts = append(texts, fmt.Sprintf("%s %s: %s %s database %q catalog %q schema %q name %q", binding.GetId(), binding.GetEntryKey(), table.GetEngine(),
			table.GetKind(), table.GetDatabase(), table.GetCatalog(), table.GetSchema(), table.GetName()))
	}
	return texts
}

func TestEncodeCoverageRegions_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name    string
		kind    schemaext.Kind
		readBy  string
		regions []*protocol.ModelNamespaceKnowledge
		// databases name the database of each native object the document
		// holds.
		databases []string
		want      []*protocol.ModelNamespaceKnowledge
	}{
		{name: "a schema", kind: encodedJobKind, readBy: "example", regions: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main"))},
			want: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main"))}},
		{name: "several schemas", kind: encodedJobKind, readBy: "example",
			regions: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main")), complete(namespace("/opened", "", "audit"))},
			want:    []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "audit")), complete(namespace("/opened", "", "main"))}},
		{name: "one table's columns", kind: encodedJobKind, readBy: "example", regions: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main", "orders"))},
			want: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main", "orders"))}},
		{name: "a path below a catalog", kind: encodedJobKind, readBy: "example", regions: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "catalog", "main", "a/b", "c"))},
			want: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "catalog", "main", "a/b", "c"))}},
		{name: "an uninspected region", kind: encodedJobKind, readBy: "example",
			regions: []*protocol.ModelNamespaceKnowledge{{Scope: namespace("/opened", "", "main"), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "a child failed"}}},
			want:    []*protocol.ModelNamespaceKnowledge{{Scope: namespace("/opened", "", "main"), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "a child failed"}}}},
		{name: "a selected kind in the database its objects name", kind: encodedQueueKind, readBy: "example", databases: []string{"/opened"},
			regions: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main"))},
			want:    []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main"))}},
		{name: "a selected kind's whole database", kind: encodedQueueKind, readBy: "example", databases: []string{"/opened"},
			regions: []*protocol.ModelNamespaceKnowledge{complete(databaseOnly("/opened"))},
			want:    []*protocol.ModelNamespaceKnowledge{complete(databaseOnly("/opened"))}},
		{name: "a selected kind in a document that holds none of its objects", kind: encodedQueueKind, readBy: "example",
			regions: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main"))},
			want:    []*protocol.ModelNamespaceKnowledge{complete(withoutDatabase(namespace("/opened", "", "main")))}},
		{name: "a selected kind whose objects name two databases", kind: encodedQueueKind, readBy: "example", databases: []string{"/opened", "/other"},
			regions: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main"))},
			want:    []*protocol.ModelNamespaceKnowledge{complete(withoutDatabase(namespace("/opened", "", "main")))}},
		{name: "a region of another engine", kind: encodedJobKind, readBy: "other", regions: []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "main"))}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			host := regionHost(c)
			known := documentCoverage(c, host, map[schemaext.Kind]schemaext.KnowledgeDomain{row.kind: readKnowledge(c, host, row.readBy, row.kind, row.regions)})
			batch := host.targets["example"].encodeBatch(protocol.SourceDocument_GO_ANNOTATIONS)
			for _, database := range row.databases {
				_, err := batch.add(protocol.SourceModelValue_NATIVE_OBJECT, "", encodedValue(row.kind,
					&protocol.Reference{Engine: "example", Kind: string(row.kind), Database: database, Schema: "main", Name: "jobs"}))
				c.Assert(err, qt.IsNil)
			}

			err := batch.coverage(known)

			c.Assert(err, qt.IsNil)
			c.Assert(coveredKinds(batch.request.GetCoverage()), qt.DeepEquals, []string{"kind " + string(row.kind)})
			c.Assert(batch.request.GetCoverage().GetKinds()[0].GetKnowledge().GetState(), qt.Equals, protocol.ModelKnowledge_UNINSPECTED)
			c.Assert(batch.request.GetCoverage().GetKinds()[0].GetKnowledge().GetReason(), qt.Equals, "outside the listed directories")
			c.Assert(regionTexts(batch.request.GetCoverage().GetKinds()[0].GetRegions()), qt.DeepEquals, regionTexts(row.want))
		})
	}
}

// TestEncodeCoverageRegions_SelectedKindInTheDatabaseOfAnotherKind spells a
// selected kind's region in a document that holds no object: the database is
// the one the region of a kind no selection names was read in.
func TestEncodeCoverageRegions_SelectedKindInTheDatabaseOfAnotherKind(t *testing.T) {
	c := qt.New(t)
	host := regionHost(c)
	region := []*protocol.ModelNamespaceKnowledge{complete(databaseOnly("/opened"))}
	known := documentCoverage(c, host, map[schemaext.Kind]schemaext.KnowledgeDomain{
		encodedJobKind: readKnowledge(c, host, "example", encodedJobKind, region), encodedQueueKind: readKnowledge(c, host, "example", encodedQueueKind, region)})
	batch := host.targets["example"].encodeBatch(protocol.SourceDocument_HCL)

	err := batch.coverage(known)

	c.Assert(err, qt.IsNil)
	c.Assert(coveredKinds(batch.request.GetCoverage()), qt.DeepEquals, []string{"kind " + string(encodedJobKind), "kind " + string(encodedQueueKind)})
	for _, kind := range batch.request.GetCoverage().GetKinds() {
		c.Assert(regionTexts(kind.GetRegions()), qt.DeepEquals, regionTexts(region))
	}
}

func TestEncodeCoverageRegions_FailurePath(t *testing.T) {
	c := qt.New(t)
	host := regionHost(c)
	outside := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "outside the listed directories"}
	whole := complete(namespace("/opened", "", "main"))
	// The scopes a read of the region whole holds: the job objects', and the
	// common tables' and columns'.
	scopes, err := namespaceScopes(host.selections, regionSemantics(c, host), "example", encodedJobKind, whole.GetScope())
	c.Assert(err, qt.IsNil)
	c.Assert(scopes, qt.HasLen, 2)
	native := schemaext.NamespaceKnowledge{Scope: scopes[0], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}
	common := schemaext.NamespaceKnowledge{Scope: scopes[1], Knowledge: schemaext.Knowledge{State: schemaext.Complete}}
	scoped := func(regions ...schemaext.NamespaceKnowledge) schemaext.KnowledgeDomain {
		domain, err := schemaext.NewScopedKnowledge(outside, regions...)
		c.Assert(err, qt.IsNil)
		return domain
	}
	differing := common
	differing.Knowledge = schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not listed"}
	everyEngine := []schemaext.NamespaceKnowledge{native, common}
	for i := range everyEngine {
		everyEngine[i].Scope.Targets = nil
	}
	exact := native
	exact.Scope.PathExact = true
	other := readKnowledge(c, host, "example", encodedJobKind, []*protocol.ModelNamespaceKnowledge{complete(namespace("/opened", "", "audit"))})
	joined, err := scoped(native, common).Intersect(other)
	c.Assert(err, qt.IsNil)
	// A selected kind's objects hold no database, so a region of them that
	// names one is no region a read holds.
	queueScopes, err := namespaceScopes(host.selections, regionSemantics(c, host), "example", encodedQueueKind, whole.GetScope())
	c.Assert(err, qt.IsNil)
	queueScopes[0].Database = &objectidentity.Part{Source: "/opened", Normalized: "/opened"}
	queueWithDatabase := []schemaext.NamespaceKnowledge{{Scope: queueScopes[0], Knowledge: native.Knowledge}, {Scope: queueScopes[1], Knowledge: native.Knowledge}}
	// The scopes a read would hold of a region that names no namespace.
	unboundedScopes, err := namespaceScopes(host.selections, regionSemantics(c, host), "example", encodedJobKind, &protocol.ModelNamespace{})
	c.Assert(err, qt.IsNil)
	var unbounded []schemaext.NamespaceKnowledge
	for _, scope := range unboundedScopes {
		unbounded = append(unbounded, schemaext.NamespaceKnowledge{Scope: scope, Knowledge: native.Knowledge})
	}
	for _, row := range []struct {
		name    string
		kind    schemaext.Kind
		domain  schemaext.KnowledgeDomain
		wantErr string
	}{
		{name: "common tables without the kind's own objects", kind: encodedJobKind, domain: scoped(common),
			wantErr: `unsupported feature: the document's knowledge of model "example.org/job" is narrowed to a namespace that no provider region spells, which an encoding request cannot carry`},
		{name: "the kind's own objects without the common tables of their region", kind: encodedJobKind, domain: scoped(native),
			wantErr: `.* model "example.org/job" is narrowed to a namespace that no provider region spells.*`},
		{name: "scopes of one region that know differently", kind: encodedJobKind, domain: scoped(native, differing),
			wantErr: `.* model "example.org/job" is narrowed to a namespace that no provider region spells.*`},
		{name: "a region of every engine", kind: encodedJobKind, domain: scoped(everyEngine...),
			wantErr: `.* model "example.org/job" is narrowed to a namespace that no provider region spells.*`},
		{name: "a path that has to match whole", kind: encodedJobKind, domain: scoped(exact, common),
			wantErr: `.* model "example.org/job" is narrowed to a namespace that no provider region spells.*`},
		{name: "independent sources", kind: encodedJobKind, domain: joined,
			wantErr: `unsupported feature: the document's knowledge of model "example.org/job" combines independent sources, which an encoding request cannot carry`},
		{name: "a selected kind's region that names a database", kind: encodedQueueKind, domain: scoped(queueWithDatabase...),
			wantErr: `.* model "example.org/queue" is narrowed to a namespace that no provider region spells.*`},
		{name: "a region without a boundary", kind: encodedJobKind, domain: scoped(unbounded...),
			wantErr: `.* model "example.org/job" is narrowed to a namespace that no provider region spells.*`},
		{name: "a selected kind's whole database in a document that names none", kind: encodedQueueKind,
			domain:  readKnowledge(c, host, "example", encodedQueueKind, []*protocol.ModelNamespaceKnowledge{complete(databaseOnly("/opened"))}),
			wantErr: `.* model "example.org/queue" is narrowed to a namespace that no provider region spells.*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			known := documentCoverage(c, host, map[schemaext.Kind]schemaext.KnowledgeDomain{row.kind: row.domain})
			batch := host.targets["example"].encodeBatch(protocol.SourceDocument_GO_ANNOTATIONS)

			err := batch.coverage(known)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(coveredKinds(batch.request.GetCoverage()), qt.HasLen, 0)
		})
	}
}

func TestEncodeBatchBindsTablesInTheirDatabase_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name string
		// owners are the owners of the facets bound to the table orders,
		// and database the database its binding names.
		owners   []*protocol.Reference
		database string
	}{
		{name: "a read's table facet", owners: []*protocol.Reference{{Engine: "example", Kind: "table", Database: "/opened", Schema: "main", Name: "orders"}},
			database: "/opened"},
		{name: "a read's column facet and table facet", owners: []*protocol.Reference{
			{Engine: "example", Kind: "column", Database: "/opened", Schema: "main", Path: []string{"orders"}, Name: "id"},
			{Engine: "example", Kind: "table", Database: "/opened", Schema: "main", Name: "orders"}},
			database: "/opened"},
		{name: "a source's table facet", owners: []*protocol.Reference{{Engine: "example", Kind: "table", Schema: "main", Name: "orders"}}},
		{name: "no facet"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			batch := regionHost(c).targets["example"].encodeBatch(protocol.SourceDocument_GO_ANNOTATIONS)
			binding := batch.bind("main", "orders")

			for _, owner := range row.owners {
				_, err := batch.add(protocol.SourceModelValue_FACET, binding, encodedValue(encodedJobKind, owner))
				c.Assert(err, qt.IsNil)
			}

			c.Assert(bindingTexts(batch.request.GetBindings()), qt.DeepEquals,
				[]string{fmt.Sprintf(`table-0 orders: example table database %q catalog "" schema "main" name "orders"`, row.database)})
		})
	}
}

func TestEncodeBatchBindsTablesInTheirDatabase_FailurePath(t *testing.T) {
	c := qt.New(t)
	batch := regionHost(c).targets["example"].encodeBatch(protocol.SourceDocument_GO_ANNOTATIONS)
	binding := batch.bind("main", "orders")
	_, err := batch.add(protocol.SourceModelValue_FACET, binding, encodedValue(encodedJobKind,
		&protocol.Reference{Engine: "example", Kind: "table", Database: "/opened", Schema: "main", Name: "orders"}))
	c.Assert(err, qt.IsNil)

	id, err := batch.add(protocol.SourceModelValue_FACET, binding, encodedValue(encodedJobKind,
		&protocol.Reference{Engine: "example", Kind: "column", Database: "/other", Schema: "main", Path: []string{"orders"}, Name: "id"}))

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `invalid feature value: the facets of table "orders" name the databases "/opened" and "/other", and a source binds a table in one`)
	c.Assert(id, qt.Equals, "")
	c.Assert(bindingTexts(batch.request.GetBindings()), qt.DeepEquals, []string{`table-0 orders: example table database "/opened" catalog "" schema "main" name "orders"`})
	c.Assert(valueIDs(batch.request.GetValues()), qt.DeepEquals, []string{"value-0"})
}

// subjectTexts writes each subject of a request's coverage as a line, as
// regionTexts writes regions.
func subjectTexts(coverage *protocol.ModelCoverage) []string {
	var texts []string
	for _, subject := range coverage.GetSubjects() {
		for _, ref := range []*protocol.Reference{subject.GetSubject(), subject.GetNormalizedSubject()} {
			texts = append(texts, fmt.Sprintf("%s %s: database %q schema %q path %v name %q", subject.GetKind(), ref.GetKind(), ref.GetDatabase(), ref.GetSchema(),
				ref.GetPath(), ref.GetName()))
		}
	}
	return texts
}

// TestEncodeCoverageSubjects spells the subjects the host holds without the
// database they were read in, a table, a column and an object of a selected
// kind, in the database a read's values name, and leaves a subject that names
// its database as it is.
func TestEncodeCoverageSubjects(t *testing.T) {
	part := scopePart
	table := objectidentity.ID{Kind: objectidentity.KindTable, Schema: part("main"), Name: part("orders")}
	column := objectidentity.ID{Kind: objectidentity.KindColumn, Schema: part("main"), Parent: part("orders"), Name: part("id")}
	queue := objectidentity.ID{Kind: objectidentity.Kind(encodedQueueKind), Schema: part("main"), Name: part("jobs")}
	job := objectidentity.ID{Kind: objectidentity.Kind(encodedJobKind), Database: part("/kept"), Schema: part("main"), Name: part("runs")}
	for _, row := range []struct {
		name string
		// databases name the database of each native object the document
		// holds.
		databases []string
		want      string
	}{
		{name: "a read names the database it was made in", databases: []string{"/opened"}, want: "/opened"},
		{name: "a source names none", want: ""},
		{name: "a document of two databases names none", databases: []string{"/opened", "/other"}, want: ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			host := regionHost(c)
			var subjects []schemaext.SubjectCoverage
			for _, record := range []struct {
				kind    schemaext.Kind
				subject objectidentity.ID
			}{{encodedJobKind, table}, {encodedJobKind, column}, {encodedQueueKind, queue}, {encodedJobKind, job}} {
				subjects = append(subjects, schemaext.SubjectCoverage{Kind: record.kind, Subject: record.subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
			}
			var kinds []schemaext.KindCoverage
			for _, kind := range []schemaext.Kind{encodedJobKind, encodedQueueKind} {
				kinds = append(kinds, schemaext.KindCoverage{Model: schemaext.CodecIdentity{Owner: host.description.GetProviderId(), Kind: kind,
					Representation: schemaext.Desired, Version: 1, Definition: "definition"}, Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete})})
			}
			known, err := schemaext.NewCoverage(schemaext.Desired, kinds, subjects)
			c.Assert(err, qt.IsNil)
			batch := host.targets["example"].encodeBatch(protocol.SourceDocument_GO_ANNOTATIONS)
			for _, database := range row.databases {
				_, err := batch.add(protocol.SourceModelValue_NATIVE_OBJECT, "", encodedValue(encodedQueueKind,
					&protocol.Reference{Engine: "example", Kind: string(encodedQueueKind), Database: database, Schema: "main", Name: "jobs"}))
				c.Assert(err, qt.IsNil)
			}

			err = batch.coverage(known)

			c.Assert(err, qt.IsNil)
			c.Assert(subjectTexts(batch.request.GetCoverage()), qt.DeepEquals, []string{
				fmt.Sprintf(`example.org/job column: database %q schema "main" path [orders] name "id"`, row.want),
				fmt.Sprintf(`example.org/job column: database %q schema "main" path [orders] name "id"`, row.want),
				`example.org/job example.org/job: database "/kept" schema "main" path [] name "runs"`,
				`example.org/job example.org/job: database "/kept" schema "main" path [] name "runs"`,
				fmt.Sprintf(`example.org/job table: database %q schema "main" path [] name "orders"`, row.want),
				fmt.Sprintf(`example.org/job table: database %q schema "main" path [] name "orders"`, row.want),
				fmt.Sprintf(`example.org/queue example.org/queue: database %q schema "main" path [] name "jobs"`, row.want),
				fmt.Sprintf(`example.org/queue example.org/queue: database %q schema "main" path [] name "jobs"`, row.want),
			})
		})
	}
}
