package sqlext_test

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
)

// bucketReader reads `BUCKET <name> ...` as an index named after the bucket.
type bucketReader struct{}

func (bucketReader) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	values := make([]sqlext.Value, 0, len(request.Claims))
	for _, claim := range request.Claims {
		values = append(values, sqlext.Value{Index: &sqlext.Index{Name: claim.Text}})
	}
	return sqlext.Result{Values: values}, nil
}

// ExampleNewSet shows an owner registering a piece of syntax as data: the
// frontend asks the set which patterns a document of a target consults at a
// site, matches the words at hand against them, and hands every span it
// claimed to the owner in one call.
func ExampleNewSet() {
	set, err := sqlext.NewSet(sqlext.Extension{
		Owner:   "example.org/custom",
		Targets: []string{"custom"},
		Patterns: []sqlext.Pattern{
			{Name: "bucket", Site: sqlext.TableElement, Words: []string{"BUCKET"}, NotNext: []string{"KEY"}, Targeted: true},
		},
		Parser: bucketReader{},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	candidate := set.Candidates("custom", sqlext.TableElement)[0]
	fmt.Println(candidate.Matches([]string{"bucket", "by_day"}))
	fmt.Println(candidate.Matches([]string{"BUCKET", "KEY"}))
	fmt.Println(len(set.Candidates("", sqlext.TableElement)))

	values, err := set.Parse(context.Background(), "custom", []sqlext.Match{
		{Candidate: candidate, Claim: sqlext.Claim{Pattern: "bucket", Site: sqlext.TableElement, Text: "BUCKET by_day ON day"}},
	}, sqlext.Document{})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(values[0].Index.Name)
	// Output:
	// true
	// false
	// 0
	// BUCKET by_day ON day
}

// ExampleSet_Parse_combined shows an owner grouping statements into one model
// declaration while preserving every statement's independent operation payload.
func ExampleSet_Parse_combined() {
	set := gadgetSet(sqlext.Statement, combinedParser{values: []sqlext.Value{
		{Statement: &gadgetStatement{}, TableReferences: []string{"orders"}, DescribedBy: 2},
		{Statement: &gadgetStatement{}, Objects: []schemaext.Object{gadgetObject(gadgetKind, "grouped")}},
	}})
	candidate := set.Candidates("x", sqlext.Statement)[0]
	values, err := set.Parse(context.Background(), "x", []sqlext.Match{
		{Candidate: candidate, Claim: sqlext.Claim{Pattern: "gadget", Site: sqlext.Statement, Text: "CREATE GADGET first"}},
		{Candidate: candidate, Claim: sqlext.Claim{Pattern: "gadget", Site: sqlext.Statement, Text: "CREATE GADGET second"}},
	}, sqlext.Document{})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("operations:", len(values))
	fmt.Println("first operation described by claim:", values[0].DescribedBy)
	fmt.Println("declaration:", values[1].Objects[0].Ref.Name.Source)
	// Output:
	// operations: 2
	// first operation described by claim: 2
	// declaration: grouped
}
