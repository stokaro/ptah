package pgrole_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgrole"
)

func roleSubject(name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).Role(name)
}

func settingsCoverage(representation schemaext.Representation, state schemaext.KnowledgeState) schemaext.Coverage {
	return must.Must(pgrole.SettingsCoverage(representation, schemaext.Knowledge{State: state, Reason: "source knowledge"}, nil))
}

func TestWithSettings_HappyPath(t *testing.T) {
	c := qt.New(t)
	facets, err := pgrole.WithSettings(schemaext.Facets{}, []pgrole.Setting{
		{Name: "work_mem", Reset: true}, {Name: "app.tenant", Value: "'acme'"}, {Name: "statement_timeout", Value: "'30s'"},
	})
	c.Assert(err, qt.IsNil)
	settings, found, err := pgrole.SettingsOf(facets)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(settings, qt.DeepEquals, []pgrole.Setting{
		{Name: "app.tenant", Value: "'acme'"}, {Name: "statement_timeout", Value: "'30s'"}, {Name: "work_mem", Reset: true},
	})
}

func TestWithSettings_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		settings []pgrole.Setting
		want     string
	}{
		{name: "named twice", settings: []pgrole.Setting{{Name: "work_mem", Value: "'1MB'"}, {Name: "WORK_MEM", Reset: true}}, want: `.*named twice`},
		{name: "no value", settings: []pgrole.Setting{{Name: "work_mem"}}, want: `.*needs a value`},
		{name: "reset with a value", settings: []pgrole.Setting{{Name: "work_mem", Value: "'1MB'", Reset: true}}, want: `.*a reset and holds no value`},
		{name: "default", settings: []pgrole.Setting{{Name: "work_mem", Value: "DEFAULT"}}, want: `.*holds DEFAULT, which is a reset`},
		{name: "not normalized", settings: []pgrole.Setting{{Name: "search_path", Value: "a,b"}}, want: `.*not normalized`},
		{name: "unquoted unit", settings: []pgrole.Setting{{Name: "statement_timeout", Value: "30s"}}, want: `.*a value with a unit is a string literal.*`},
		{name: "no name", settings: []pgrole.Setting{{Value: "'1MB'"}}, want: `.*needs a parameter name`},
		{name: "empty part", settings: []pgrole.Setting{{Name: "app..tenant", Value: "'1MB'"}}, want: `.*not held as a name is read`},
		{name: "statement in a value", settings: []pgrole.Setting{{Name: "work_mem", Value: "'1MB'; DROP ROLE x"}}, want: `.*expected a comma.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			facets, err := pgrole.WithSettings(schemaext.Facets{}, test.settings)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(facets.IsZero(), qt.IsTrue)
		})
	}
}

func TestChanges(t *testing.T) {
	for _, test := range []struct {
		name               string
		declared, observed []pgrole.Setting
		want               *pgrole.SettingsChange
	}{
		{name: "set where none is held", declared: []pgrole.Setting{{Name: "statement_timeout", Value: "'30s'"}},
			want: &pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "statement_timeout", After: new("'30s'")}}}},
		{name: "held as declared", declared: []pgrole.Setting{{Name: "statement_timeout", Value: "'30s'"}},
			observed: []pgrole.Setting{{Name: "statement_timeout", Value: "30s"}}},
		{name: "another spelling of the value", declared: []pgrole.Setting{{Name: "statement_timeout", Value: "'30s'"}},
			observed: []pgrole.Setting{{Name: "statement_timeout", Value: "30000"}},
			want:     &pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "statement_timeout", Before: new("30000"), After: new("'30s'")}}}},
		{name: "list held as declared", declared: []pgrole.Setting{{Name: "search_path", Value: "'$user', public"}},
			observed: []pgrole.Setting{{Name: "search_path", Value: `"$user", public`}}},
		{name: "reset of a held setting", declared: []pgrole.Setting{{Name: "work_mem", Reset: true}},
			observed: []pgrole.Setting{{Name: "work_mem", Value: "64MB"}},
			want:     &pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "work_mem", Before: new("64MB")}}}},
		{name: "reset of a setting not held", declared: []pgrole.Setting{{Name: "work_mem", Reset: true}}},
		{name: "undeclared setting is left alone", declared: []pgrole.Setting{{Name: "statement_timeout", Value: "'30s'"}},
			observed: []pgrole.Setting{{Name: "statement_timeout", Value: "30s"}, {Name: "work_mem", Value: "64MB"}}},
		{name: "names match without case", declared: []pgrole.Setting{{Name: "app.tenant", Value: "'acme'"}},
			observed: []pgrole.Setting{{Name: "App.Tenant", Value: "acme"}}},
		{name: "changes in name order", declared: []pgrole.Setting{{Name: "work_mem", Value: "'1MB'"}, {Name: "app.tenant", Value: "'acme'"}},
			want: &pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "app.tenant", After: new("'acme'")}, {Name: "work_mem", After: new("'1MB'")}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgrole.Changes(test.declared, test.observed), qt.DeepEquals, test.want)
		})
	}
}

func TestAlterSettingsStatements_HappyPath(t *testing.T) {
	c := qt.New(t)
	operation := &pgrole.AlterSettings{Role: `App "Reader"`, Change: pgrole.SettingsChange{Settings: []pgrole.SettingChange{
		{Name: "app.Tenant", Before: new("acme")},
		{Name: "search_path", After: new("'$user', public")},
		{Name: "statement_timeout", Before: new("1min"), After: new("'30s'")},
	}}}
	statements, err := operation.Statements()
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`ALTER ROLE "App ""Reader""" RESET "app"."Tenant";`,
		`ALTER ROLE "App ""Reader""" SET "search_path" = '$user', public;`,
		`ALTER ROLE "App ""Reader""" SET "statement_timeout" = '30s';`,
	})
	c.Assert(operation.Subject(), qt.DeepEquals, roleSubject(`App "Reader"`))
}

func TestAlterSettingsStatements_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name      string
		operation *pgrole.AlterSettings
		want      string
	}{
		{name: "nil", want: `.*nil role settings operation`},
		{name: "no role", operation: &pgrole.AlterSettings{Change: pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "work_mem", After: new("'1MB'")}}}},
			want: `.*requires a role`},
		{name: "no setting", operation: &pgrole.AlterSettings{Role: "app"}, want: `.*requires a setting`},
		{name: "out of order", operation: &pgrole.AlterSettings{Role: "app", Change: pgrole.SettingsChange{Settings: []pgrole.SettingChange{
			{Name: "work_mem", After: new("'1MB'")}, {Name: "app.tenant", After: new("'a'")}}}}, want: `.*once, in name order`},
		{name: "to the value held", operation: &pgrole.AlterSettings{Role: "app", Change: pgrole.SettingsChange{Settings: []pgrole.SettingChange{
			{Name: "work_mem", Before: new("1MB"), After: new("'1MB'")}}}}, want: `.*changes to the value it holds`},
		{name: "value that is not SQL", operation: &pgrole.AlterSettings{Role: "app", Change: pgrole.SettingsChange{Settings: []pgrole.SettingChange{
			{Name: "work_mem", After: new("1MB; DROP ROLE app")}}}}, want: `.*a value with a unit is a string literal.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := test.operation.Statements()
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestCreationOperation_HappyPath(t *testing.T) {
	c := qt.New(t)
	facets := must.Must(pgrole.WithSettings(schemaext.Facets{}, []pgrole.Setting{{Name: "work_mem", Reset: true}, {Name: "statement_timeout", Value: "'30s'"}}))
	operation, err := pgrole.CreationOperation("app", facets)
	c.Assert(err, qt.IsNil)
	statements, err := operation.Statements()
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{`ALTER ROLE "app" SET "statement_timeout" = '30s';`})
}

// A new role holds no setting, so a declaration of resets alone asks nothing
// of it, and a role without the facet manages none.
func TestCreationOperation_NothingToSet(t *testing.T) {
	for _, test := range []struct {
		name   string
		facets schemaext.Facets
	}{
		{name: "resets only", facets: must.Must(pgrole.WithSettings(schemaext.Facets{}, []pgrole.Setting{{Name: "work_mem", Reset: true}}))},
		{name: "no facet"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operation, err := pgrole.CreationOperation("app", test.facets)
			c.Assert(err, qt.IsNil)
			c.Assert(operation, qt.IsNil)
		})
	}
}

func TestSettingsChangeReverse_HappyPath(t *testing.T) {
	c := qt.New(t)
	change := &pgrole.SettingsChange{Settings: []pgrole.SettingChange{
		{Name: "search_path", Before: new(`"$user", public`)},
		{Name: "statement_timeout", After: new("'30s'")},
		{Name: "work_mem", Before: new("64MB"), After: new("'1MB'")},
	}}
	reversed, err := change.Reverse()
	c.Assert(err, qt.IsNil)
	c.Assert(reversed, qt.DeepEquals, &pgrole.SettingsChange{Settings: []pgrole.SettingChange{
		{Name: "search_path", After: new("'$user', 'public'")},
		{Name: "statement_timeout", Before: new("30s")},
		{Name: "work_mem", Before: new("1MB"), After: new("'64MB'")},
	}})
	again, err := reversed.Reverse()
	c.Assert(err, qt.IsNil)
	c.Assert(again.Settings[0].Before, qt.DeepEquals, new(`"$user", public`))
}

// A RESET a SQL document writes knows no value before it, so nothing can
// restore one; the reversal is refused rather than written as a SET of
// something invented.
func TestSettingsChangeReverse_FailurePath(t *testing.T) {
	c := qt.New(t)
	change := &pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "work_mem"}}}
	reversed, err := change.Reverse()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*held before its reset is not known`)
	c.Assert(reversed, qt.IsNil)
}

func TestSettingsCodecs_HappyPath(t *testing.T) {
	for _, test := range []struct {
		codec schemaext.Codec
		text  string
	}{
		{pgrole.SettingsCodecs()[0], `{"settings":[{"name":"statement_timeout","value":"'30s'"},{"name":"work_mem","reset":true}]}`},
		{pgrole.SettingsCodecs()[0], `{"settings":[]}`},
		{pgrole.SettingsCodecs()[1], `{"settings":[{"name":"search_path","value":"\"$user\", public"}]}`},
		{pgrole.SettingsChangeCodec(), `{"settings":[{"name":"work_mem","before":"64MB","after":null}]}`},
		{pgrole.SettingsChangeCodec(), `{"settings":[{"name":"work_mem","before":null,"after":null}]}`},
		{pgrole.OperationCodec(), `{"role":"app","change":{"settings":[{"name":"work_mem","before":null,"after":"'1MB'"}]}}`},
	} {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			value, err := test.codec.Decode(json.RawMessage(test.text))
			c.Assert(err, qt.IsNil)
			encoded, err := test.codec.Encode(value)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.JSONEquals, json.RawMessage(test.text))
			copied, err := test.codec.Clone(value)
			c.Assert(err, qt.IsNil)
			c.Assert(copied, qt.DeepEquals, value)
		})
	}
}

func TestSettingsCodecs_FailurePath(t *testing.T) {
	for _, test := range []struct {
		codec schemaext.Codec
		text  string
	}{
		{pgrole.SettingsCodecs()[0], `{}`},
		{pgrole.SettingsCodecs()[0], `{"settings":null}`},
		{pgrole.SettingsCodecs()[0], `{"settings":[{"name":"work_mem"}]}`},
		{pgrole.SettingsCodecs()[0], `{"settings":[{"name":"work_mem","value":"'1MB'","extra":1}]}`},
		{pgrole.SettingsCodecs()[0], `{"settings":[{"name":"work_mem","value":"1MB"}]}`},
		{pgrole.SettingsCodecs()[1], `{"settings":[{"name":"work_mem","reset":true}]}`},
		{pgrole.SettingsChangeCodec(), `{"settings":[]}`},
		{pgrole.SettingsChangeCodec(), `{"settings":[{"name":"work_mem","after":"'1MB'"}]}`},
		{pgrole.OperationCodec(), `{"change":{"settings":[{"name":"work_mem","before":null,"after":"'1MB'"}]}}`},
	} {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			value, err := test.codec.Decode(json.RawMessage(test.text))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestSettingsService_ConvertFeatures_HappyPath(t *testing.T) {
	c := qt.New(t)
	service := pgrole.SettingsService{}
	declared := must.Must(pgrole.Declare([]pgrole.Setting{{Name: "search_path", Value: "'$user', public"}, {Name: "statement_timeout", Value: "'30s'"}, {Name: "work_mem", Reset: true}}))
	observed, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{declared}})
	c.Assert(err, qt.IsNil)
	c.Assert(observed, qt.DeepEquals, []schemaext.Value{&pgrole.ObservedSettings{Settings: []pgrole.Setting{
		{Name: "search_path", Value: `"$user", public`}, {Name: "statement_timeout", Value: "30s"},
	}}})
	restored, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired, Values: observed})
	c.Assert(err, qt.IsNil)
	c.Assert(restored, qt.DeepEquals, []schemaext.Value{&pgrole.DesiredSettings{Settings: []pgrole.Setting{
		{Name: "search_path", Value: "'$user', 'public'"}, {Name: "statement_timeout", Value: "'30s'"},
	}}})
}

func TestSettingsService_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		name   string
		ctx    context.Context
		target string
		want   error
	}{
		{name: "other target", ctx: t.Context(), target: "cockroachdb", want: ptaherr.ErrUnsupportedDialect},
		{name: "canceled", ctx: canceled, target: "postgres", want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := pgrole.SettingsService{}
			converted, err := service.ConvertFeatures(test.ctx, schemaext.ConversionRequest{Target: test.target, From: schemaext.Desired, To: schemaext.Observed})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(converted, qt.IsNil)
			compared, err := service.CompareFacets(test.ctx, schemaext.FacetComparisonRequest{Target: test.target})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(compared, qt.DeepEquals, schemaext.FacetComparisonResult{})
			planned, err := service.PlanFeatures(test.ctx, featureplan.Request{Target: test.target})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(planned, qt.DeepEquals, featureplan.Result{})
			reversed, err := service.ReverseChanges(test.ctx, schemaext.ReversalRequest{Target: test.target})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(reversed, qt.IsNil)
		})
	}
}

func TestSettingsService_CompareFacets(t *testing.T) {
	declared := must.Must(pgrole.WithSettings(schemaext.Facets{}, []pgrole.Setting{{Name: "statement_timeout", Value: "'30s'"}, {Name: "work_mem", Reset: true}}))
	held := must.Must(pgrole.WithObservedSettings(schemaext.Facets{}, []pgrole.Setting{{Name: "statement_timeout", Value: "1min"}, {Name: "work_mem", Value: "64MB"}, {Name: "app.tenant", Value: "acme"}}))
	converged := must.Must(pgrole.WithObservedSettings(schemaext.Facets{}, []pgrole.Setting{{Name: "statement_timeout", Value: "30s"}, {Name: "app.tenant", Value: "acme"}}))
	change := &pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "statement_timeout", Before: new("1min"), After: new("'30s'")}, {Name: "work_mem", Before: new("64MB")}}}
	for _, test := range []struct {
		name               string
		desired, current   schemaext.Coverage
		declared, observed schemaext.Facets
		created, removed   bool
		want               []schemaext.FacetChange
		undecided          []schemaext.UndecidedChange
	}{
		{name: "declared settings change", desired: settingsCoverage(schemaext.Desired, schemaext.Complete), current: settingsCoverage(schemaext.Observed, schemaext.Complete),
			declared: declared, observed: held, want: []schemaext.FacetChange{{Kind: pgrole.SettingsKind, Change: schemaext.ChangeRecord{Subject: roleSubject("app"), Value: change}}}},
		{name: "converged", desired: settingsCoverage(schemaext.Desired, schemaext.Complete), current: settingsCoverage(schemaext.Observed, schemaext.Complete),
			declared: declared, observed: converged},
		{name: "role without settings declared manages none", desired: settingsCoverage(schemaext.Desired, schemaext.Complete),
			current: settingsCoverage(schemaext.Observed, schemaext.Complete), observed: held},
		{name: "complete read without the facet holds none", desired: settingsCoverage(schemaext.Desired, schemaext.Complete), current: settingsCoverage(schemaext.Observed, schemaext.Complete),
			declared: declared, want: []schemaext.FacetChange{{Kind: pgrole.SettingsKind, Change: schemaext.ChangeRecord{Subject: roleSubject("app"),
				Value: &pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "statement_timeout", After: new("'30s'")}}}}}}},
		{name: "read that does not describe settings", desired: settingsCoverage(schemaext.Desired, schemaext.Complete), declared: declared,
			undecided: []schemaext.UndecidedChange{{Kind: pgrole.SettingsKind, Subject: roleSubject("app"), Reason: "the current source does not describe role settings"}}},
		{name: "uninspected read", desired: settingsCoverage(schemaext.Desired, schemaext.Complete), current: settingsCoverage(schemaext.Observed, schemaext.Uninspected),
			declared: declared, observed: held, undecided: []schemaext.UndecidedChange{{Kind: pgrole.SettingsKind, Subject: roleSubject("app"), Reason: "source knowledge"}}},
		{name: "new role carries its settings in its creation", declared: declared, created: true},
		{name: "dropped role loses its settings with it", observed: held, removed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.FacetComparisonRequest{Target: "postgres", Kinds: []schemaext.Kind{pgrole.SettingsKind},
				Desired: schemaext.FacetState{Coverage: test.desired, Records: []schemaext.FacetRecord{{Subject: roleSubject("app"), Values: test.declared}}},
				Current: schemaext.FacetState{Coverage: test.current, Records: []schemaext.FacetRecord{{Subject: roleSubject("app"), Values: test.observed}}},
				Owners:  []schemaext.ParentState{{Subject: roleSubject("app"), Desired: !test.removed, Current: !test.created}}}
			got, err := (pgrole.SettingsService{}).CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Changes, qt.DeepEquals, test.want)
			c.Assert(got.Undecided, qt.DeepEquals, test.undecided)
			c.Assert(got.Complete, qt.IsTrue)
		})
	}
}

func TestSettingsService_PlanFeatures_HappyPath(t *testing.T) {
	c := qt.New(t)
	change := &pgrole.SettingsChange{Settings: []pgrole.SettingChange{{Name: "statement_timeout", After: new("'30s'")}}}
	result, err := (pgrole.SettingsService{}).PlanFeatures(t.Context(), featureplan.Request{Target: "postgres",
		Changes: []schemaext.ChangeRecord{{Subject: roleSubject(`App "Reader"`), Value: change}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions, qt.HasLen, 1)
	steps := result.Contributions[0].Steps
	c.Assert(steps, qt.HasLen, 1)
	c.Assert(steps[0].Payload.Phase, qt.Equals, featureplan.PhaseDependent)
	statements, err := steps[0].Payload.Payload.(*pgrole.AlterSettings).Statements()
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{`ALTER ROLE "App ""Reader""" SET "statement_timeout" = '30s';`})
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Changes[0].Steps, qt.DeepEquals, []plangraph.StepID{steps[0].ID})
}
