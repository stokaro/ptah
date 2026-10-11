package sqlext_test

import (
	"context"
	"fmt"

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
	})
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
