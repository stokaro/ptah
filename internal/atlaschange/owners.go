package atlaschange

import (
	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgtype"
)

// describeOwner names an operation of a built-in feature owner. The community
// binary plans none of these objects, so they take the pattern of the objects it
// does plan: `create "hstore" extension`, `drop "p" policy`, `modify "posint"
// domain`, `set comment to extension: "hstore"`.
//
// A switch of a row-level security flag changes the table, which is how the
// statement names it, so it reads `modify "t" table`.
func describeOwner(payload ast.ExtensionPayload) (change, bool) {
	switch op := payload.(type) {
	case *pgextension.Operation:
		return change{verb: reportedVerb(op), kind: "extension", name: op.Name}, true
	case *pgextension.CommentOperation:
		return change{verb: comment, kind: "extension", name: op.Name}, true
	case *pgpolicy.PolicyOperation:
		return change{verb: reportedVerb(op), kind: "policy", name: op.Name}, true
	case *pgpolicy.PolicyCommentOperation:
		return change{verb: comment, kind: "policy", name: op.Name}, true
	case *pgpolicy.TableStateOperation:
		return change{verb: modify, kind: kindTable, name: op.Table}, true
	case *pgtype.CreateDomain:
		return change{verb: create, kind: "domain", name: op.Target.Name}, true
	case *pgtype.AlterDomain:
		return change{verb: modify, kind: "domain", name: op.Target.Name}, true
	case *pgtype.CreateComposite:
		return change{verb: create, kind: "composite type", name: op.Target.Name}, true
	case *pgtype.AlterComposite:
		return change{verb: modify, kind: "composite type", name: op.Target.Name}, true
	case *pgtype.CreateRange:
		return change{verb: create, kind: "range type", name: op.Target.Name}, true
	case *pgtype.Drop:
		return change{verb: drop, kind: typeWords[op.Target.Kind], name: op.Target.Name}, true
	case *pgtype.Comment:
		return change{verb: comment, kind: typeWords[op.Target.Kind], name: op.Target.Name}, true
	}
	return describeReported(payload)
}

// typeWords names a PostgreSQL type by its kind.
var typeWords = map[schemaext.Kind]string{
	pgtype.DomainKind:    "domain",
	pgtype.CompositeKind: "composite type",
	pgtype.RangeKind:     "range type",
}

// reportedVerb is the verb of the action an operation reports.
func reportedVerb(reporter ast.ExtensionChangeReporter) verb {
	switch reporter.SchemaChange().Action {
	case ast.ExtensionAdd:
		return create
	case ast.ExtensionDrop:
		return drop
	default:
		return modify
	}
}

// subjectReporter is an operation that says what it changes for a schema-change
// report, and which object a skip line names.
type subjectReporter interface {
	ast.ExtensionChangeReporter
	OmissionSubject() (kind, name string)
}

// describeReported names an operation of another owner from what the operation
// reports about itself: `create "metrics" hypertable`. An operation that does not
// report it is not named.
func describeReported(payload ast.ExtensionPayload) (change, bool) {
	reporter, ok := payload.(subjectReporter)
	if !ok {
		return change{}, false
	}
	kind, name := reporter.OmissionSubject()
	if kind == "" || name == "" {
		return change{}, false
	}
	return change{verb: reportedVerb(reporter), kind: kind, name: bare(name)}, true
}
