// Package tssource decodes TimescaleDB's Go annotation directives into the
// owner's models: a hypertable, which partitions the table it names, and a
// continuous aggregate, a materialized view TimescaleDB keeps up to date.
//
// The Go annotation frontend reads these directives only when the caller
// selects this owner, through [Annotations] in the runtime's annotation set.
package tssource

import (
	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
)

// The directives the owner declares.
const (
	// HypertableDirective declares a hypertable.
	HypertableDirective = "ptah:schema:hypertable"
	// ContinuousAggregateDirective declares a continuous aggregate.
	ContinuousAggregateDirective = "ptah:schema:continuousaggregate"
)

// Annotations is the owner's contribution to the Go annotation frontend: both
// directives, their decoder, and the claim that a Go annotation source
// describes every hypertable and continuous aggregate it holds, so an omitted
// one is absent.
//
// There is no dialect scope on either directive: both belong to TimescaleDB
// and to nothing else.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner:      tsschema.Owner,
		Directives: []annotation.Directive{continuousAggregateDirective(), hypertableDirective()},
		Kinds:      []schemaext.Kind{tsschema.ContinuousAggregateKind, tsschema.HypertableKind},
		Decode:     decode,
		Coverage:   annotation.Unlimited(func() (schemaext.Coverage, error) { return tsschema.CompleteCoverage(schemaext.Desired) }),
	}
}

func decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	kv := declaration.Attributes
	if declaration.Directive == HypertableDirective {
		// chunk_interval is kept as the string it was written as. The catalog
		// reports the server's own spelling -- `7 days`, `1 day` -- and a
		// declaration converted to something else to compare would differ
		// from it on every run.
		return []annotation.Contribution{{Table: kv["table"], Label: "a hypertable", Facet: &tsschema.DesiredHypertable{
			Column:        kv["column"],
			ChunkInterval: kv["chunk_interval"],
			IfNotExists:   kv["if_not_exists"] == "true",
			Comment:       kv["comment"],
		}}}, nil
	}
	// The body is kept as it was written. The catalog stores a rewritten
	// SELECT, and a live comparison puts the declaration through the same
	// rewrite rather than folding either text -- so a declaration normalized
	// here would be normalized twice and match nothing.
	schemaName, name := annotation.QualifiedName(kv["schema"], kv["name"])
	object := tsschema.DesiredContinuousAggregateObject(schemaName, name, tsschema.DesiredContinuousAggregate{
		Body:             kv["body"],
		MaterializedOnly: optionalBool(kv, "materialized_only"),
		Comment:          kv["comment"],
		StructName:       declaration.Struct,
	})
	return []annotation.Contribution{{Object: &object,
		Label: "continuous aggregate \"" + tsschema.QualifiedName(object.Ref) + "\""}}, nil
}

// optionalBool answers nil for an attribute the annotation did not write, so
// a reader can tell "unset" from "set to false".
func optionalBool(kv map[string]string, name string) *bool {
	written, present := kv[name]
	if !present {
		return nil
	}
	return new(written == "true")
}

func continuousAggregateDirective() annotation.Directive {
	return annotation.Directive{
		Name: ContinuousAggregateDirective,
		Description: "Declares a TimescaleDB continuous aggregate: a materialized view over a " +
			"hypertable the extension keeps up to date.",
		Scopes: []annotation.Scope{annotation.ScopeStruct},
		Attributes: []annotation.Attribute{
			{Name: "name", Description: "Aggregate name, which is also the view name.", Value: "string", Required: true},
			{Name: "schema", Description: "Schema holding the aggregate.", Value: "string"},
			{Name: "body", Description: "The SELECT the aggregate materializes.", Value: "string", Required: true},
			{Name: "materialized_only", Description: "Read only materialized data, rather than combining it with " +
				"the rows since the last refresh.", Value: "boolean"},
			{Name: "comment", Description: "Continuous aggregate comment.", Value: "string"},
		},
	}
}

func hypertableDirective() annotation.Directive {
	return annotation.Directive{
		Name: HypertableDirective,
		Description: "Declares a TimescaleDB hypertable: a table partitioned on a range " +
			"dimension.",
		Scopes: []annotation.Scope{annotation.ScopeStruct},
		Attributes: []annotation.Attribute{
			{Name: "table", Description: "Table to partition, optionally schema-qualified.", Value: "string", Required: true},
			{Name: "column", Description: "Range dimension: the column chunks are cut on.", Value: "string", Required: true},
			{Name: "chunk_interval", Description: "Width of one chunk, spelled the way PostgreSQL spells an " +
				"interval. Omit to take TimescaleDB's default.", Value: "string"},
			{Name: "if_not_exists", Description: "Skip a table that is already a hypertable instead of failing.",
				Value: "boolean"},
			{Name: "comment", Description: "Hypertable comment.", Value: "string"},
		},
	}
}
