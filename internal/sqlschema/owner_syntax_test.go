package sqlschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/engine"
	"ptah.run/internal/sqlschema"
)

// bucketParser is an owner of target "custom" that reads `BUCKET <name> ON
// <expression>` as an index, inside a column list and as an ALTER TABLE
// operation, and records the requests it is given.
type bucketParser struct {
	requests *[]sqlext.Request
}

func (p bucketParser) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	*p.requests = append(*p.requests, request)
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		fields := strings.Fields(strings.TrimPrefix(claim.Text, "ADD "))
		index := &sqlext.Index{Name: fields[1], Expression: strings.Join(fields[3:], " ")}
		value := sqlext.Value{Index: index}
		if claim.Site == sqlext.AlterOperation {
			value.Operation = &bucketOperation{Name: index.Name}
		}
		values = append(values, value)
	}
	return sqlext.Result{Values: values}, nil
}

// bucketOperation is the owner's payload of an ALTER TABLE ... ADD BUCKET.
type bucketOperation struct {
	Name string
}

func (*bucketOperation) Kind() schemaext.Kind { return "example.org/custom/add-bucket" }

func (o *bucketOperation) CloneExtension() ast.ExtensionPayload { return new(*o) }

// bucketRuntime registers target "custom" with the bucket owner's syntax.
func bucketRuntime(requests *[]sqlext.Request) *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID:      "example.org/custom",
		Targets: []engine.Target{{Name: "custom"}},
		SQL: []sqlext.Extension{{
			Owner:   "example.org/custom",
			Targets: []string{"custom"},
			Patterns: []sqlext.Pattern{
				{Name: "bucket", Site: sqlext.TableElement, Words: []string{"BUCKET"}, Targeted: true},
				{Name: "add-bucket", Site: sqlext.AlterOperation, Words: []string{"ADD", "BUCKET"}, Targeted: true},
			},
			Parser: bucketParser{requests: requests},
		}},
	}))
}

const bucketDocument = "CREATE TABLE events (id INTEGER, day INTEGER, BUCKET by_day ON day);\n" +
	"ALTER TABLE events ADD BUCKET by_id ON id + 1;\n"

// TestReadWith_ReachesTheTargetsSQLOwner shows that a read hands the spans an
// owner's patterns claim to the owner registered on the selected target, in
// one call for the document, and applies the indexes it reads: the one a
// table element declares and the one an ALTER TABLE operation adds.
func TestReadWith_ReachesTheTargetsSQLOwner(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request

	database, statements, err := sqlschema.ReadWith(t.Context(), []byte(bucketDocument), "custom", nil,
		sqlschema.Options{Owners: bucketRuntime(&requests).SQL()})

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Target, qt.Equals, "custom")
	c.Assert(requests[0].Claims, qt.HasLen, 2)
	c.Assert(requests[0].Claims[0].Text, qt.Equals, "BUCKET by_day ON day")
	c.Assert(requests[0].Claims[0].Subject, qt.Equals, "events")
	c.Assert(requests[0].Claims[1].Text, qt.Equals, "ADD BUCKET by_id ON id + 1")
	c.Assert(bucketIndexes(database.Indexes), qt.DeepEquals, []string{"by_day day", "by_id id + 1"})
	alter, isAlter := statements.Statements[1].(*ast.AlterTableNode)
	c.Assert(isAlter, qt.IsTrue)
	operation, isOwned := alter.Operations[0].(*ast.ExtensionAlterOperation)
	c.Assert(isOwned, qt.IsTrue)
	c.Assert(operation.Payload, qt.DeepEquals, ast.ExtensionPayload(&bucketOperation{Name: "by_id"}))
}

// TestReadWith_WithoutTheOwnerReadsTheSharedGrammar is the control: without
// the owner the bucket is no syntax the shared grammar knows, and the read
// refuses the document rather than read it as something else.
func TestReadWith_WithoutTheOwnerReadsTheSharedGrammar(t *testing.T) {
	c := qt.New(t)

	_, _, err := sqlschema.ReadWith(t.Context(), []byte(bucketDocument), "custom", nil,
		sqlschema.Options{Owners: sqlext.None()})

	c.Assert(err, qt.IsNotNil)
}

// TestReadWith_RefusesAnUnselectedOwnerSet refuses the zero set, so a caller
// that forgot to select owners is told rather than read without them.
func TestReadWith_RefusesAnUnselectedOwnerSet(t *testing.T) {
	c := qt.New(t)

	_, statements, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE t (id INTEGER);"), "postgres", nil, sqlschema.Options{})

	c.Assert(err, qt.ErrorIs, sqlext.ErrUnselected)
	c.Assert(statements, qt.IsNil)
}

// bucketIndexes lists each index as its name and its one field.
func bucketIndexes(indexes []schemamodel.Index) []string {
	listed := make([]string, 0, len(indexes))
	for _, index := range indexes {
		listed = append(listed, index.Name+" "+strings.Join(index.Fields, ","))
	}
	return listed
}
