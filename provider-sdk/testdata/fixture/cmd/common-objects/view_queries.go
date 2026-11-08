package main

import (
	"context"
	"os"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// viewQueries reports FIXTURE_VIEW_QUERIES=exact or context, which makes the
// program compare view queries itself (schema.view-query-equivalence) and
// keep the case of every word, as an engine whose names keep their case does.
// Without it the host compares the queries by its own reading, which folds
// case.
func viewQueries() bool { return os.Getenv("FIXTURE_VIEW_QUERIES") == "exact" || viewQueryContext() }

// viewQueryContext reports FIXTURE_VIEW_QUERIES=context, under which the
// program also lists schema.view-query-context. It then reads `SELECT *` as
// the declared columns of its table, as an engine that stores a star query
// with the columns named must, and calls such a query another one when the
// host sent no columns for the table. A pair whose words differ in case alone
// it leaves to the host, which folds case.
func viewQueryContext() bool { return os.Getenv("FIXTURE_VIEW_QUERIES") == "context" }

// CompareViewQueries calls a declared query and a held one one query when
// their words match, case included. The program's read joins the runs of
// white space of a held query into one space, so it reads both sides so.
func (p *commonObjects) CompareViewQueries(ctx context.Context, request *protocol.CompareViewQueriesRequest) (*protocol.ComparedViewQueries, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &protocol.ComparedViewQueries{Complete: true}
	for _, pair := range request.GetPairs() {
		answer := &protocol.ComparedViewQuery{Identity: proto.CloneOf(pair.GetCurrent().GetIdentity())}
		declared, held := foldSpaces(pair.GetDeclared()), foldSpaces(pair.GetCurrent().GetBody())
		if viewQueryContext() {
			declared, held = expandStar(declared, request.GetDeclaredColumns()), expandStar(held, request.GetDeclaredColumns())
			answer.Undecided = declared != held && strings.EqualFold(declared, held)
		}
		answer.Equivalent = !answer.GetUndecided() && declared != "" && declared == held
		result.Queries = append(result.Queries, answer)
	}
	return result, nil
}

// expandStar writes `SELECT * FROM <table>` with the declared columns of the
// table named, and as nothing when the request carries none for it. Any other
// query is returned as it is.
func expandStar(query string, columns []*protocol.ComparisonColumn) string {
	table, star := strings.CutPrefix(query, "SELECT * FROM ")
	if !star {
		return query
	}
	var names []string
	for _, column := range columns {
		if column.GetTable().GetName().GetSource() == table {
			names = append(names, column.GetName())
		}
	}
	if len(names) == 0 {
		return ""
	}
	return "SELECT " + strings.Join(names, ", ") + " FROM " + table
}
