package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine"
)

func externalPlanningRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	kinds := []schemaext.Kind{ydbexternal.SourceKind, ydbexternal.TableKind}
	changes := []schemaext.Kind{ydbdiff.ExternalDataSourceKind, ydbdiff.ExternalTableKind}
	operations := []schemaext.Kind{ydbast.ExternalDataSourceKind, ydbast.ExternalTableKind}
	codecs := append(ydbexternal.Codecs(), ydbdiff.ExternalDataSourceCodec(), ydbdiff.ExternalTableCodec(),
		ydbast.ExternalDataSourceCodec(), ydbast.ExternalTableCodec())
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: codecs,
		Declarations: []engine.DeclarationPlanning{{Target: "ydb", Kinds: kinds, OperationKinds: operations, Service: ydbplan.ExternalService{}}},
		Planning:     []engine.Planning{{Target: "ydb", Kinds: changes, OperationKinds: operations, Service: ydbplan.ExternalService{}}}})
	c.Assert(err, qt.IsNil)
	return runtime
}

var (
	plannedBucket = ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"}
	plannedEvents = ydbexternal.Table{DataSource: "ext/bucket", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
)

// externalCaps is 26.2 with external data sources on, and CREATE OR REPLACE
// of them when replace is set.
func externalCaps(replace bool) capability.Capabilities {
	return capability.YDB262().With(capability.ExternalDataSources, true).With(capability.ExternalObjectReplace, replace)
}

func sourceRecord(schema, name string, change *ydbdiff.ExternalDataSource) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbexternal.SourceRef(schema, name), Value: change}
}

func tableRecord(schema, name string, change *ydbdiff.ExternalTable) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbexternal.TableRef(schema, name), Value: change}
}

func externalRequest(caps capability.Capabilities, records ...schemaext.ChangeRecord) featureplan.Request {
	return featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: caps, DatabasePath: "/local", Changes: records}
}

// TestExternalPlan_LowersEachChange writes the statements each change needs,
// with the object's identity and its path as effects, the change's effect as
// the impact, and a strategy that says what the statements do. A changed
// source is replaced in place where the target can and recreated in one step
// where it cannot; a changed table is dropped and created in two.
func TestExternalPlan_LowersEachChange(t *testing.T) {
	moved := plannedBucket.Clone()
	moved.Location = "https://s3.example.test/other/"
	tests := []struct {
		name     string
		replace  bool
		record   schemaext.ChangeRecord
		want     []string
		strategy string
	}{
		{name: "a created source", record: sourceRecord("ext", "bucket", &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: plannedBucket}}),
			want: []string{"create source ext/bucket"}, strategy: "create the data source"},
		{name: "a dropped source", record: sourceRecord("ext", "bucket", &ydbdiff.ExternalDataSource{Before: &ydbexternal.ObservedSource{Spec: plannedBucket}}),
			want: []string{"drop source ext/bucket"}, strategy: "drop the data source; the system it reads is untouched"},
		{name: "a source replaced", replace: true, record: sourceRecord("ext", "bucket", &ydbdiff.ExternalDataSource{
			Before: &ydbexternal.ObservedSource{Spec: plannedBucket}, After: &ydbexternal.DesiredSource{Spec: moved}}),
			want: []string{"replace source ext/bucket"}, strategy: "replace the data source in place with CREATE OR REPLACE"},
		{name: "a source recreated", record: sourceRecord("ext", "bucket", &ydbdiff.ExternalDataSource{
			Before: &ydbexternal.ObservedSource{Spec: plannedBucket}, After: &ydbexternal.DesiredSource{Spec: moved}}),
			want: []string{"recreate source ext/bucket"}, strategy: "drop the data source and create it again, with the external tables over it"},
		{name: "a created table", record: tableRecord("ext", "events", &ydbdiff.ExternalTable{After: &ydbexternal.DesiredTable{Spec: plannedEvents}}),
			want: []string{"create table ext/events"}, strategy: "create the external table"},
		{name: "a dropped table", record: tableRecord("ext", "events", &ydbdiff.ExternalTable{Before: &ydbexternal.ObservedTable{Spec: plannedEvents}}),
			want: []string{"drop table ext/events"}, strategy: "drop the external table; the files it reads stay"},
		{name: "a table replaced", replace: true, record: tableRecord("ext", "events", &ydbdiff.ExternalTable{
			Before: &ydbexternal.ObservedTable{Spec: plannedEvents}, After: &ydbexternal.DesiredTable{Spec: plannedEvents}}),
			want: []string{"replace table ext/events"}, strategy: "replace the external table in place with CREATE OR REPLACE"},
		{name: "a table recreated", record: tableRecord("ext", "events", &ydbdiff.ExternalTable{
			Before: &ydbexternal.ObservedTable{Spec: plannedEvents}, After: &ydbexternal.DesiredTable{Spec: plannedEvents}}),
			want: []string{"drop table ext/events", "create table ext/events"}, strategy: "drop the external table and create it again"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := externalRequest(externalCaps(test.replace), test.record)

			result, err := externalPlanningRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(scheduledNames(c, commonChain(), result), qt.DeepEquals, test.want)
			c.Assert(result.Changes[0].Strategy, qt.Equals, test.strategy)
			c.Assert(result.Changes[0].Steps, qt.HasLen, len(test.want))
			for _, step := range result.Contributions[0].Steps {
				subject := step.Payload.Payload.(interface{ Subject() objectidentity.ID }).Subject()
				c.Assert(step.Effects[0].Subject, qt.DeepEquals, test.record.Subject)
				c.Assert(step.Effects[1].Subject, qt.DeepEquals, ydbscheme.Path(subject.Schema.Source, subject.Name.Source))
				c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
				c.Assert(step.Placement, qt.Equals, plangraph.PlacementEarly)
				c.Assert(step.Impact, qt.DeepEquals, step.Payload.Payload.(interface{ Effect() schemaext.Effect }).Effect())
			}
		})
	}
}

// TestExternalPlan_OrdersTheBatch drops an external table before the data
// source it read and creates it after the one it reads, and gives each
// statement what it reads: a data source the secrets its _SECRET_PATH options
// name, read against the database root, and an external table its source. A
// statement nothing orders comes in owner and name order.
func TestExternalPlan_OrdersTheBatch(t *testing.T) {
	c := qt.New(t)
	moved := plannedBucket.Clone()
	moved.Location = "https://s3.example.test/other/"
	warehouse := ydbexternal.DataSource{SourceType: "PostgreSQL", AuthMethod: "BASIC",
		Options: map[string]string{"LOGIN": "u", "PASSWORD_SECRET_PATH": "/local/ext/pw", "TOKEN_SECRET_PATH": "ext/pw"}} // #nosec G101 -- secret paths, not credentials
	request := externalRequest(externalCaps(false),
		tableRecord("ext", "events", &ydbdiff.ExternalTable{Before: &ydbexternal.ObservedTable{Spec: plannedEvents}, After: &ydbexternal.DesiredTable{Spec: plannedEvents}}),
		sourceRecord("ext", "bucket", &ydbdiff.ExternalDataSource{Before: &ydbexternal.ObservedSource{Spec: plannedBucket}, After: &ydbexternal.DesiredSource{Spec: moved}}),
		sourceRecord("ext", "warehouse", &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: warehouse}}),
	)

	result, err := externalPlanningRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(scheduledNames(c, commonChain(), result), qt.DeepEquals, []string{
		"create source ext/warehouse", "drop table ext/events", "recreate source ext/bucket", "create table ext/events",
	})
	reads := make(map[string][]plangraph.Effect)
	for _, step := range result.Contributions[0].Steps {
		reads[step.ID.Name] = step.Effects[2:]
	}
	c.Assert(reads, qt.DeepEquals, map[string][]plangraph.Effect{
		"external-table/000000/drop":         {},
		"external-data-source/000001/alter":  {},
		"external-data-source/000002/create": {{Subject: ydbsecret.Ref("ext", "pw"), Action: plangraph.Read}},
		"external-table/000003/create":       {{Subject: ydbexternal.SourceRef("ext", "bucket"), Action: plangraph.Read}},
	})
}

// TestExternalPlan_PlacesEachStatement orders an external object against the
// host's statements: first, unless a drop frees its path or a directory above
// it, and a drop before a creation at its path.
func TestExternalPlan_PlacesEachStatement(t *testing.T) {
	slot := ydbscheme.Path("ext", "bucket")
	tests := []struct {
		name    string
		change  *ydbdiff.ExternalDataSource
		effects [][]plangraph.Effect
		want    []string
	}{
		{name: "a creation runs first", change: &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: plannedBucket}},
			effects: [][]plangraph.Effect{nil, nil}, want: []string{"create source ext/bucket", "a", "b"}},
		{name: "a creation follows the drop that frees its path", change: &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: plannedBucket}},
			effects: [][]plangraph.Effect{nil, {{Subject: slot, Action: plangraph.Drop}}, nil},
			want:    []string{"a", "b", "create source ext/bucket", "c"}},
		{name: "a creation follows the drop of its directory", change: &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: plannedBucket}},
			effects: [][]plangraph.Effect{nil, {{Subject: ydbscheme.Path("", "ext"), Action: plangraph.Drop}}, nil},
			want:    []string{"a", "b", "create source ext/bucket", "c"}},
		{name: "a drop runs before a creation at its path", change: &ydbdiff.ExternalDataSource{Before: &ydbexternal.ObservedSource{Spec: plannedBucket}},
			effects: [][]plangraph.Effect{nil, {{Subject: slot, Action: plangraph.Create}}},
			want:    []string{"drop source ext/bucket", "a", "b"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			chain := commonChain(test.effects...)
			request := externalRequest(externalCaps(false), sourceRecord("ext", "bucket", test.change))
			request.CommonSteps = chain.steps

			result, err := externalPlanningRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(scheduledNames(c, chain, result), qt.DeepEquals, test.want)
		})
	}
}

// TestExternalPlan_RefusesTheWholeBatch returns no statement when one change
// is refused: a target without the key, a secret path on a line that reads it
// as a name, a secret path outside the database, an object at a path another
// object of the batch or of the host takes, and an external table over a
// source that is not object storage.
func TestExternalPlan_RefusesTheWholeBatch(t *testing.T) {
	created := func(spec ydbexternal.DataSource) *ydbdiff.ExternalDataSource {
		return &ydbdiff.ExternalDataSource{After: &ydbexternal.DesiredSource{Spec: spec}}
	}
	withPassword := func(path string) ydbexternal.DataSource {
		return ydbexternal.DataSource{SourceType: "PostgreSQL", AuthMethod: "BASIC", Options: map[string]string{"PASSWORD_SECRET_PATH": path}}
	}
	tests := []struct {
		name    string
		caps    capability.Capabilities
		records []schemaext.ChangeRecord
		effects [][]plangraph.Effect
		code    schemavalidation.Code
		wantErr string
	}{
		{name: "a line without the key", caps: capability.YDB262(), records: []schemaext.ChangeRecord{sourceRecord("ext", "bucket", created(plannedBucket))},
			code:    schemavalidation.UnsupportedFeature,
			wantErr: "external data source ext/bucket, which requires target capability external_data_sources, unavailable on this ydb target"},
		{name: "a drop on a line without the key", caps: capability.YDB262(),
			records: []schemaext.ChangeRecord{tableRecord("ext", "events", &ydbdiff.ExternalTable{Before: &ydbexternal.ObservedTable{Spec: plannedEvents}})},
			code:    schemavalidation.UnsupportedFeature,
			wantErr: "DROP EXTERNAL TABLE ext/events, which requires target capability external_data_sources, unavailable on this ydb target"},
		{name: "a secret path on 25.1", caps: capability.YDB251().With(capability.ExternalDataSources, true),
			records: []schemaext.ChangeRecord{sourceRecord("ext", "pg", created(withPassword("ext/pw")))}, code: schemavalidation.UnsupportedFeature,
			wantErr: "external data source ext/pg option PASSWORD_SECRET_PATH, which requires target capability external_data_source_secret_paths, .*"},
		{name: "a secret path outside the database", caps: externalCaps(false),
			records: []schemaext.ChangeRecord{sourceRecord("ext", "pg", created(withPassword("/other/pw")))}, code: schemavalidation.InvalidSchema,
			wantErr: `external data source ext/pg: option PASSWORD_SECRET_PATH names secret "/other/pw", which is outside the database /local, ` +
				`so the data source cannot read it`},
		{name: "two objects at one path", caps: externalCaps(false), records: []schemaext.ChangeRecord{
			sourceRecord("ext", "events", created(plannedBucket)),
			tableRecord("ext", "events", &ydbdiff.ExternalTable{After: &ydbexternal.DesiredTable{Spec: plannedEvents}})},
			code:    schemavalidation.InvalidSchema,
			wantErr: "external table ext/events has the path of external data source ext/events, and YDB keeps one object at a path \\(`unexpected path type`\\)"},
		{name: "a table over a PostgreSQL source", caps: externalCaps(false), records: []schemaext.ChangeRecord{
			sourceRecord("ext", "bucket", created(withPassword("ext/pw"))),
			tableRecord("ext", "events", &ydbdiff.ExternalTable{After: &ydbexternal.DesiredTable{Spec: plannedEvents}})},
			code: schemavalidation.InvalidSchema,
			wantErr: "external table ext/events reads data source ext/bucket, a PostgreSQL source; an external table reads files, " +
				"from an ObjectStorage source \\(`Only ObjectStorage source type supported`\\)"},
		{name: "a path a table takes", caps: externalCaps(false), records: []schemaext.ChangeRecord{sourceRecord("ext", "bucket", created(plannedBucket))},
			effects: [][]plangraph.Effect{{{Subject: ydbscheme.Path("ext", "bucket"), Action: plangraph.Create}}},
			code:    schemavalidation.InvalidSchema, wantErr: ".*external data source create conflicts with create at scheme path.*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := externalRequest(test.caps, test.records...)
			request.CommonSteps = commonChain(test.effects...).steps

			result, err := externalPlanningRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, test.code)
			c.Assert(result.Err(request), qt.ErrorMatches, test.wantErr)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestExternalDeclarations_CreateEachDeclaredObject derives one creation per
// declared data source and external table, a table after its source, and
// names the model kind in a refusal.
func TestExternalDeclarations_CreateEachDeclaredObject(t *testing.T) {
	c := qt.New(t)
	request := featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: externalCaps(false),
		Objects: []schemaext.Object{
			ydbexternal.DesiredTableObject("ext", "events", "", plannedEvents),
			ydbexternal.DesiredSourceObject("ext", "bucket", "", plannedBucket),
		}}

	result, err := externalPlanningRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	c.Assert(result.Declarations, qt.HasLen, 2)
	c.Assert([]string{result.Declarations[0].Strategy, result.Declarations[1].Strategy}, qt.DeepEquals,
		[]string{"create the declared external table", "create the declared data source"})
	c.Assert(scheduledNames(c, commonChain(), featureplan.Result{Contributions: result.Contributions}), qt.DeepEquals,
		[]string{"create source ext/bucket", "create table ext/events"})
	refused, err := externalPlanningRuntime(c).PlanDeclarations(t.Context(), featureplan.DeclarationRequest{Target: "ydb",
		Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(), Objects: request.Objects[:1]})
	c.Assert(err, qt.IsNil)
	c.Assert(refused.Diagnostics, qt.HasLen, 1)
	c.Assert(refused.Diagnostics[0].Problem.Kind, qt.Equals, string(ydbexternal.TableKind))
}
