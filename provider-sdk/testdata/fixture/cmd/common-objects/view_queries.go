package main

import (
	"context"
	"os"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// viewQueries reports FIXTURE_VIEW_QUERIES=exact, which makes the program
// compare view queries itself (schema.view-query-equivalence) and keep the
// case of every word, as an engine whose names keep their case does. Without
// it the host compares the queries by its own reading, which folds case.
func viewQueries() bool { return os.Getenv("FIXTURE_VIEW_QUERIES") == "exact" }

// CompareViewQueries calls a declared query and a held one one query when
// their words match, case included. The program's read joins the runs of
// white space of a held query into one space, so it reads both sides so.
func (p *commonObjects) CompareViewQueries(ctx context.Context, request *protocol.CompareViewQueriesRequest) (*protocol.ComparedViewQueries, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &protocol.ComparedViewQueries{Complete: true}
	for _, pair := range request.GetPairs() {
		result.Queries = append(result.Queries, &protocol.ComparedViewQuery{Identity: proto.CloneOf(pair.GetCurrent().GetIdentity()),
			Equivalent: foldSpaces(pair.GetDeclared()) == foldSpaces(pair.GetCurrent().GetBody())})
	}
	return result, nil
}
