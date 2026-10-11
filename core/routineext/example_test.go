package routineext_test

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/core/routineext"
)

// lineReader is a reader for a toy language whose body is one statement per
// line, and in which a line beginning RUN composes its statement at run time.
type lineReader struct{}

func (lineReader) ReadBodies(_ context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	result := routineext.ReadResult{Bodies: make([]routineext.Reading, 0, len(request.Bodies))}
	for _, body := range request.Bodies {
		var reading routineext.Reading
		for line := range strings.SplitSeq(strings.TrimSpace(body.SQL), "\n") {
			statement := routineext.Statement{Kind: routineext.StatementSQL, SQL: line}
			if strings.HasPrefix(line, "RUN ") {
				statement = routineext.Statement{Kind: routineext.StatementDynamic, Label: "run", SQL: line}
			}
			reading.Statements = append(reading.Statements, statement)
		}
		result.Bodies = append(result.Bodies, reading)
	}
	return result, nil
}

// ExampleValidateRead shows a host asking an owner to read the bodies of one
// document in one call, checking that the answer is one, and finding where
// static analysis of each body stops.
func ExampleValidateRead() {
	request := routineext.ReadRequest{Target: "toy", Bodies: []routineext.Body{
		{Kind: "procedure", SQL: "DELETE FROM sessions\nRUN 'DROP TABLE ' || name"},
		{Kind: "function", SQL: "SELECT 1"},
	}}
	result, err := lineReader{}.ReadBodies(context.Background(), request)
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := routineext.ValidateRead(request, result); err != nil {
		fmt.Println(err)
		return
	}
	for i, reading := range result.Bodies {
		for _, statement := range reading.Statements {
			fmt.Println(i, statement.Kind, statement.SQL)
		}
	}
	// Output:
	// 0 sql DELETE FROM sessions
	// 0 dynamic RUN 'DROP TABLE ' || name
	// 1 sql SELECT 1
}
