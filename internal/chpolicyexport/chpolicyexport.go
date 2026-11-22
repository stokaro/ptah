// Package chpolicyexport prepares the ClickHouse row policies of a database
// read for a schema written from it, which is what ptah db read, ptah schema
// inspect and ptah introspect do. A read holds two kinds of policy that no
// source can declare, and this package says what an export does with each
// rather than letting the writer refuse the whole schema.
//
// A policy whose table the read does not hold is one ClickHouse kept when the
// table was dropped: the server binds a policy to a table name, so the policy
// applies to the next table created under that name. The read holds it as an
// object, which a comparison plans for, and no source can declare it without
// its table. [Without] leaves it out of the schema and warns about it, naming
// the statement that drops it: a source describes every row policy, so
// applying the written schema drops the policy.
//
// A policy the read could not describe, such as one written ON db.* or one on
// a view, is a coverage record rather than an object, so the writer of each
// format leaves it out. A SQL export names it in a comment and an HCL export
// in its header; [ReportUndescribed] names it for a format that has no place
// for either, such as Go annotations. A comparison of the database keeps it.
package chpolicyexport

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chrender"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/chpolicysource"
	"ptah.run/internal/rundiag"
)

// Without returns schema without the ClickHouse row policies it holds for a
// table it does not hold, and warns on w about each, naming the DROP ROW
// POLICY statement that removes it. A ClickHouse read describes the tables of
// one database and names a policy's table in that database, so a policy is
// matched to a table by name. A schema holding no such policy is returned
// unchanged, and so is a nil one. The result shares nothing it changes with
// schema.
func Without(ctx context.Context, w io.Writer, schema *catalog.Database) (*catalog.Database, error) {
	if schema == nil {
		return nil, nil
	}
	held := make(map[string]bool, len(schema.Tables))
	for _, table := range schema.Tables {
		held[table.Name] = true
	}
	objects, err := schema.FeatureObjects.All()
	if err != nil {
		return nil, err
	}
	var outlived []schemaext.Object
	for _, object := range objects {
		if object.Ref.Kind == objectidentity.Kind(chschema.RowPolicyKind) && !held[object.Ref.Parent.Source] {
			outlived = append(outlived, object)
		}
	}
	if len(outlived) == 0 {
		return schema, nil
	}
	left := make(map[objectidentity.Key]bool, len(outlived))
	for _, object := range outlived {
		statement, err := dropStatement(object)
		if err != nil {
			return nil, err
		}
		left[object.Ref.Key()] = true
		named := fmt.Sprintf("ClickHouse row policy %q on table %q", object.Ref.Name.Source, object.Ref.Parent.Source)
		line := fmt.Sprintf("%s is left out: the database holds it for a table it does not hold, and applies it to the next table "+
			"created under that name; a schema applied from this output drops it with %s", named, statement)
		_ = rundiag.Warn(ctx, w, rundiag.LeftOutRowPolicies, rundiag.Record{Key: line, Line: line,
			Attrs: []slog.Attr{slog.String("object", named), slog.String("statement", statement)}})
	}
	result := *schema
	result.FeatureObjects = schema.FeatureObjects.Select(func(ref objectidentity.ID) bool { return !left[ref.Key()] })
	return &result, nil
}

// databaseWide is the table name a ClickHouse read records a policy written
// ON db.* under (see [chschema.ValidateRowPolicyRef]).
const databaseWide = "*"

// ReportUndescribed warns on w about each row policy coverage records as met
// and not described: the policies an export to a format with no comment of
// its own leaves out.
func ReportUndescribed(ctx context.Context, w io.Writer, coverage schemaext.Coverage) {
	for _, record := range chpolicysource.Undescribed(coverage) {
		named := fmt.Sprintf("ClickHouse row policy %q on %q", record.Subject.Name.Source, record.Subject.Parent.Source)
		if record.Subject.Parent.Source == databaseWide {
			named = fmt.Sprintf("ClickHouse row policy %q on every table of the database", record.Subject.Name.Source)
		}
		line := fmt.Sprintf("%s is left out: %s", named, record.Knowledge.Reason)
		_ = rundiag.Warn(ctx, w, rundiag.LeftOutRowPolicies, rundiag.Record{Key: line, Line: line,
			Attrs: []slog.Attr{slog.String("object", named), slog.String("cause", string(record.Knowledge.Reason))}})
	}
}

// dropStatement is the statement that drops the policy object holds.
func dropStatement(object schemaext.Object) (string, error) {
	observed, ok := object.Value.(*chschema.ObservedRowPolicy)
	if !ok {
		return "", fmt.Errorf("%w: ClickHouse row policy %s is %T, not an observation", schemaext.ErrInvalidValue, object.Ref, object.Value)
	}
	registry, err := chrender.Registry()
	if err != nil {
		return "", err
	}
	operation := &chast.RowPolicy{Database: chschema.RowPolicyDatabase(object.Ref), Table: object.Ref.Parent.Source,
		Name: object.Ref.Name.Source, Change: *chdiff.NewRowPolicy(observed, nil)}
	statements, err := registry.Render(renderer.ExtensionContext{Target: platform.ClickHouse,
		Capabilities: capability.Capabilities{}.With(capability.RowLevelSecurity, true)}, ast.StatementExtension, operation)
	if err != nil {
		return "", err
	}
	if len(statements) != 1 {
		return "", fmt.Errorf("%w: dropping ClickHouse row policy %s rendered %d statements", schemaext.ErrInvalidValue, object.Ref, len(statements))
	}
	return statements[0], nil
}
