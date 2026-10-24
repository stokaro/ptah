// The tests in this directory register dialects with the query builder. A
// registration lasts for the life of the process, so they live in a test
// binary of their own rather than beside the builder's other tests, and each
// registers a name no other test uses.
package registeredtest_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/core/query"
)

// spelling is the spelling of a provider's declared query dialect: question
// marks, double quotes, ON CONFLICT, no FULL JOIN, and RETURNING assumed.
func spelling() query.Spelling {
	return query.Spelling{
		Placeholder:         descriptor.PlaceholderQuestion,
		Quoting:             descriptor.Quoting{Style: descriptor.QuoteDouble},
		Query:               descriptor.Query{Upsert: descriptor.UpsertOnConflict, UnsupportedJoins: []descriptor.Join{descriptor.JoinFull}},
		DefaultCapabilities: map[capability.Capability]bool{capability.ReturningClause: true},
	}
}

func TestRegisterDialect_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(query.RegisterDialect("registered-onconflict", spelling()), qt.IsNil)
	c.Assert(query.RegisterDialect("registered-onconflict", spelling()), qt.IsNil)

	insert := query.InsertInto("regions").Columns("code", "name").Values("NO", "Norway").OnConflictDoNothing("code").Returning("code").Build()
	text, args, err := query.RenderInsert(insert, "registered-onconflict")
	c.Assert(err, qt.IsNil)
	c.Assert(text, qt.Equals, `INSERT INTO "regions" ("code", "name") VALUES (?, ?) ON CONFLICT ("code") DO NOTHING RETURNING "code"`)
	c.Assert(args, qt.DeepEquals, []any{"NO", "Norway"})

	update := query.Update("regions").Set("name", "Norge").Where(query.Eq("code", "NO")).Build()
	text, args, err = query.RenderUpdate(update, "registered-onconflict")
	c.Assert(err, qt.IsNil)
	c.Assert(text, qt.Equals, `UPDATE "regions" SET "name" = ? WHERE "code" = ?`)
	c.Assert(args, qt.DeepEquals, []any{"Norge", "NO"})
}

// The ADR 0028 case: an engine whose provider spells an upsert as UPSERT INTO,
// quotes in backticks and names its arguments.
func TestRegisterDialect_HappyPath_UpsertInto(t *testing.T) {
	c := qt.New(t)
	c.Assert(query.RegisterDialect("registered-upsert-into", query.Spelling{
		Placeholder: descriptor.PlaceholderDollarNamed,
		Quoting:     descriptor.Quoting{Style: descriptor.QuoteBacktick, Bare: descriptor.BareNever},
		Query:       descriptor.Query{Upsert: descriptor.UpsertInto},
	}), qt.IsNil)
	upsert := query.UpsertInto("regions").Columns("code", "name").Values("NO", "Norway").Build()
	text, args, err := query.RenderInsert(upsert, "registered-upsert-into")
	c.Assert(err, qt.IsNil)
	c.Assert(text, qt.Equals, "UPSERT INTO `regions` (`code`, `name`) VALUES ($p1, $p2)")
	c.Assert(args, qt.HasLen, 2)
}

// A clause the spelling cannot express is refused, and so is one the default
// capabilities do not establish.
func TestRegisteredDialect_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(query.RegisterDialect("registered-refusals", query.Spelling{
		Placeholder: descriptor.PlaceholderQuestion,
		Query:       descriptor.Query{UnsupportedJoins: []descriptor.Join{descriptor.JoinFull}},
	}), qt.IsNil)

	joined := query.Select().From("a").FullJoin("b", "", query.Expr(query.ColExpr("a.id")).Eq(query.ColExpr("b.id"))).Build()
	text, _, err := query.RenderSelect(joined, "registered-refusals")
	c.Assert(err, qt.ErrorMatches, `renderer: registered-refusals does not support FULL OUTER JOIN`)
	c.Assert(text, qt.Equals, "")

	returning := query.InsertInto("a").Columns("id").Values(1).Returning("id").Build()
	text, _, err = query.RenderInsert(returning, "registered-refusals")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(text, qt.Equals, "")

	upsert := query.UpsertInto("a").Columns("id").Values(1).Build()
	text, _, err = query.RenderInsert(upsert, "registered-refusals")
	c.Assert(err, qt.ErrorMatches, `renderer: registered-refusals has no UPSERT statement: .*`)
	c.Assert(text, qt.Equals, "")
}

// A name that already renders with another spelling keeps it.
func TestRegisterDialect_FailurePath_Registered(t *testing.T) {
	c := qt.New(t)
	c.Assert(query.RegisterDialect("registered-twice", spelling()), qt.IsNil)
	other := spelling()
	other.Placeholder = descriptor.PlaceholderDollar
	for _, row := range []struct {
		name, engine string
		spelling     query.Spelling
		wantErr      string
	}{
		{"a built-in dialect", "postgres", spelling(), `.*postgres is a dialect Ptah describes`},
		{"another spelling for a registered name", "registered-twice", other, `.*registered-twice was registered with another spelling`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			err := query.RegisterDialect(row.engine, row.spelling)
			c.Assert(err, qt.ErrorIs, query.ErrDialectRegistered)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
		})
	}
	text, _, err := query.RenderInsert(query.InsertInto("a").Columns("id").Values(1).Build(), "registered-twice")
	c.Assert(err, qt.IsNil)
	c.Assert(text, qt.Equals, `INSERT INTO "a" ("id") VALUES (?)`)
}

// A spelling the builder does not know registers nothing.
func TestRegisterDialect_FailurePath_Invalid(t *testing.T) {
	unknownCapability := spelling()
	unknownCapability.DefaultCapabilities = map[capability.Capability]bool{"no_such_capability": true}
	for _, row := range []struct {
		name, engine string
		spelling     query.Spelling
		wantErr      string
	}{
		{"an unknown placeholder", "registered-placeholder", query.Spelling{Placeholder: "percent"}, `query: the spelling of registered-placeholder: .*placeholder "percent" is not one of.*`},
		{"an unknown capability", "registered-capability", unknownCapability, `query: the default capabilities of registered-capability: unknown capability "no_such_capability"`},
		{"a name with surrounding space", " spaced", spelling(), `query: a dialect needs a name without surrounding space, got " spaced"`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(query.RegisterDialect(row.engine, row.spelling), qt.ErrorMatches, row.wantErr)
			text, _, err := query.RenderInsert(query.InsertInto("a").Columns("id").Values(1).Build(), row.engine)
			c.Assert(err, qt.ErrorMatches, `renderer: INSERT rendering is not supported for dialect .*`)
			c.Assert(text, qt.Equals, "")
		})
	}
}
