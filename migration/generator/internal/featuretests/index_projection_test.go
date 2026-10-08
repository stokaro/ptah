package featuretests_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

func indexProjectionOptions(c *qt.C) generator.BidirectionalSchemaPlanOptions {
	c.Helper()
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	before := declaration(c, stream)
	stream.RetentionPeriod = "PT12H"
	after := declaration(c, stream)
	fields := []schemamodel.Field{{StructName: "Item", Name: "label", Type: "string", Nullable: true}, {StructName: "Item", Name: "category", Type: "string", Nullable: true}}
	before.Fields, after.Fields = append(before.Fields, fields...), append(after.Fields, fields...)
	before.Indexes = []schemamodel.Index{
		{Name: "kept", StructName: "Item", Fields: []string{"label"}},
		{Name: "gone", StructName: "Item", Fields: []string{"category"}, Comment: "removed comment"},
		{Name: "old_name", StructName: "Item", Fields: []string{"label", "category"}, Comment: "old comment"},
	}
	after.Indexes = []schemamodel.Index{
		{Name: "kept", StructName: "Item", Fields: []string{"label"}},
		{Name: "fresh", StructName: "Item", Fields: []string{"category", "label"}},
		{Name: "new_name", StructName: "Item", Fields: []string{"label", "category"}, Comment: "new comment"},
	}
	return options(c, runtime, before, after)
}

func TestGeneratorProjectsAcceptedIndexLifecycleWithFeatureChanges(t *testing.T) {
	c := qt.New(t)
	opts := indexProjectionOptions(c)
	c.Assert(opts.Diff.IndexesAdded.Refs(), qt.DeepEquals, []difftypes.IndexRef{{Name: "fresh", TableName: "items"}})
	c.Assert(opts.Diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{{Name: "gone", TableName: "items"}})
	c.Assert(opts.Diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{{TableName: "items", From: "old_name", To: "new_name"}})
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	indexes := plan.Reverse.Diff.TablesModified[0].Current.Indexes
	c.Assert(indexes, qt.HasLen, 3)
	c.Assert(indexes[0].Name, qt.Equals, "kept")
	c.Assert(indexes[1].Name, qt.Equals, "new_name")
	c.Assert(indexes[1].Comment, qt.Equals, "new comment")
	c.Assert(indexes[1].Columns, qt.DeepEquals, []string{"label", "category"})
	c.Assert(indexes[2].Name, qt.Equals, "fresh")
	c.Assert(indexes[2].Columns, qt.DeepEquals, []string{"category", "label"})
	c.Assert(indexes[2].QualifiedTableName(), qt.Equals, "items")
	c.Assert(opts.Diff.TablesModified[0].Current.Indexes[1].Name, qt.Equals, "gone")
	c.Assert(opts.Diff.TablesModified[0].Current.Indexes[2].Name, qt.Equals, "old_name")
	indexes[0].Columns[0], indexes[1].Columns[0], indexes[2].Columns[0] = "mutated", "mutated", "mutated"
	c.Assert(opts.CurrentSchema.Indexes[0].Columns, qt.DeepEquals, []string{"label"})
	c.Assert(opts.Diff.TablesModified[0].Current.Indexes[2].Columns, qt.DeepEquals, []string{"label", "category"})
	c.Assert(opts.Diff.IndexesAdded[0].Index.Fields, qt.DeepEquals, []string{"category", "label"})
}

func TestGeneratorPreservesSkippedIndexAndItsComment(t *testing.T) {
	c := qt.New(t)
	opts := indexProjectionOptions(c)
	filtered, skipped := diffpolicy.ApplyForDialect(opts.Diff, diffpolicy.NewSkipSet(diffpolicy.DropIndex), "ydb")
	c.Assert(skipped, qt.DeepEquals, []diffpolicy.SkippedChange{{Kind: diffpolicy.DropIndex, Object: "items.gone"}})
	opts.Diff = filtered
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	indexes := plan.Reverse.Diff.TablesModified[0].Current.Indexes
	c.Assert(indexes, qt.HasLen, 4)
	c.Assert(indexes[1].Name, qt.Equals, "gone")
	c.Assert(indexes[1].Comment, qt.Equals, "removed comment")
	c.Assert(indexes[3].Name, qt.Equals, "fresh")
}

func TestGeneratorProjectsTheAcceptedIndexDefinitionAfterSourceMutation(t *testing.T) {
	c := qt.New(t)
	opts := indexProjectionOptions(c)
	opts.DesiredSchema.Indexes[1].Fields[0] = "unknown_column"
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
	c.Assert(err, qt.IsNil)
	indexes := plan.Reverse.Diff.TablesModified[0].Current.Indexes
	c.Assert(indexes[2].Columns, qt.DeepEquals, []string{"category", "label"})
	sql, err := builtin.RenderSQLWithCapabilities("ydb", opts.Capabilities, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ON (`category`, `label`)")
	c.Assert(sql, qt.Not(qt.Contains), "unknown_column")
}

func TestGeneratorRefusesUnresolvableIndexProjection(t *testing.T) {
	cases := []struct {
		name       string
		removals   []difftypes.IndexRef
		additions  difftypes.IndexChanges
		renames    []difftypes.IndexRename
		visibility []difftypes.IndexVisibilityChange
		comments   []difftypes.IndexCommentChange
		want       string
	}{
		{name: "missing removal", removals: []difftypes.IndexRef{{TableName: "items", Name: "missing"}}, want: "removal of missing index"},
		{name: "duplicate removal", removals: []difftypes.IndexRef{{TableName: "items", Name: "gone"}, {TableName: "items", Name: "gone"}}, want: "removal of missing index"},
		{name: "missing rename", renames: []difftypes.IndexRename{{TableName: "items", From: "missing", To: "new_name"}}, want: "conflicting rename"},
		{name: "rename collision", renames: []difftypes.IndexRename{{TableName: "items", From: "old_name", To: "kept"}}, want: "conflicting rename"},
		{name: "addition collision", additions: difftypes.IndexChanges{{TableName: "items", Index: schemamodel.Index{Name: "kept", Fields: []string{"label"}}}}, want: "conflicting addition"},
		{name: "incomplete addition", additions: difftypes.IndexChanges{{TableName: "items", Index: schemamodel.Index{Name: "empty"}}}, want: "incomplete addition"},
		{name: "missing visibility", visibility: []difftypes.IndexVisibilityChange{{TableName: "items", Name: "missing", Invisible: true}}, want: "visibility of missing index"},
		{name: "missing comment", comments: []difftypes.IndexCommentChange{{TableName: "items", Name: "missing", Desired: "x"}}, want: "comment of missing index"},
		{name: "removed index cannot receive comment", removals: []difftypes.IndexRef{{TableName: "items", Name: "gone"}}, comments: []difftypes.IndexCommentChange{{TableName: "items", Name: "gone", Desired: "x"}}, want: "comment of missing index"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := indexProjectionOptions(c)
			opts.Diff.IndexesAdded, opts.Diff.IndexesRemoved = test.additions, test.removals
			opts.Diff.IndexesRenamed, opts.Diff.IndexVisibilityChanged = test.renames, test.visibility
			opts.Diff.IndexCommentsChanged = test.comments
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
			c.Assert(err, qt.ErrorMatches, ".*cannot project "+test.want+".*")
			c.Assert(plan, qt.IsNil)
		})
	}
}

func TestGeneratorRefusesAmbiguousIndexCapture(t *testing.T) {
	cases := []struct {
		name  string
		index catalog.Index
		want  string
	}{
		{name: "duplicate identity", index: catalog.Index{TableName: "items", Name: "kept"}, want: "duplicate captured index"},
		{name: "wrong owner", index: catalog.Index{TableName: "other", Name: "elsewhere"}, want: "index .* captured under another table"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := indexProjectionOptions(c)
			capture := &opts.Diff.TablesModified[0].Current
			capture.Indexes = append(capture.Indexes, test.index)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), opts)
			c.Assert(err, qt.ErrorMatches, ".*cannot project "+test.want+".*")
			c.Assert(plan, qt.IsNil)
		})
	}
}

func TestGeneratorProjectsIndexVisibilityUnderCapturedIdentityRules(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := &schemamodel.Database{
		Tables:  []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields:  []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INT"}},
		Indexes: []schemamodel.Index{{Name: "By_ID", StructName: "Item", Fields: []string{"id"}}},
	}
	current, err := goschematodb.ToDBSchema(t.Context(), before, "mysql", runtime)
	c.Assert(err, qt.IsNil)
	before.Tables[0].Comment = "changed table"
	before.Indexes[0].Name, before.Indexes[0].Invisible = "by_id", true
	caps := capability.MySQL84()
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), before, current, catalog.ServerInfo{Dialect: "mysql", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexVisibilityChanged, qt.HasLen, 1)
	// The confirmed index identity accepts this spelling too. Its captured
	// spelling remains the database's when no rename was accepted.
	diff.IndexVisibilityChanged[0].Name = "BY_ID"
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: before, CurrentSchema: current, Dialect: "mysql", Capabilities: caps,
	})
	c.Assert(err, qt.IsNil)
	indexes := plan.Reverse.Diff.TablesModified[0].Current.Indexes
	c.Assert(indexes, qt.HasLen, 1)
	c.Assert(indexes[0].Invisible, qt.IsTrue)
	c.Assert(indexes[0].Name, qt.Equals, "By_ID")
	c.Assert(current.Indexes[0].Invisible, qt.IsFalse)
}
