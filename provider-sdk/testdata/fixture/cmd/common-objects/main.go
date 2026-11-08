// Package main serves the fixture with the common-object wire of contract
// 25.3: views, roles and groups, grants and comments, with
// FIXTURE_MATERIALIZED_VIEWS=true materialized views, and with
// FIXTURE_VIEW_QUERIES=exact or context its own comparison of view queries.
//
// SQLite has views and nothing else of these, so the program keeps roles,
// grants, comments and materialized views in a file of its own beside the
// database, which its reads report and its executor writes. Each change
// renders as one statement its own executor reads: `FIXTURE {...};`, the
// change as JSON. A view is a real SQLite view; the executor creates it with
// CREATE VIEW. Dropping a table, a view or a materialized view takes the
// grants on it, as an engine that keeps them in the object's own access list
// does.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

// commonKinds are the kinds the program carries: materialized views too where
// materializedViews says so.
func commonKinds() []protocol.CommonObjectSupport_Kind {
	kinds := []protocol.CommonObjectSupport_Kind{
		protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_ROLE, protocol.CommonObjectSupport_GRANT, protocol.CommonObjectSupport_COMMENT,
	}
	if materializedViews() {
		kinds = append(kinds, protocol.CommonObjectSupport_MATERIALIZED_VIEW)
	}
	return kinds
}

// materializedViews reports FIXTURE_MATERIALIZED_VIEWS=true, which makes the
// program carry materialized views. Without it the program refuses one, as a
// provider whose engine has none does.
func materializedViews() bool { return os.Getenv("FIXTURE_MATERIALIZED_VIEWS") == "true" }

func main() {
	if path := os.Getenv("PTAH_FIXTURE_PID_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil { // #nosec G703 -- the host selects this fixture PID file in its private test directory
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	description := backend.Description()
	description.SemanticIdentity += "-common-objects"
	added := []string{provider.CommonObjectsFeature, provider.LocksFeature}
	description.Features = append(description.Features, added...)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, added...)
	engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: commonKinds()}
	owner := &commonObjects{Provider: &backend.Provider{ViewsElsewhere: true}}
	server := runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Locks: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, CommonObjects: owner,
	}
	if viewQueries() {
		description.Features = append(description.Features, provider.ViewQueryEquivalenceFeature)
		engine.Features = append(engine.Features, provider.ViewQueryEquivalenceFeature)
		server.ViewComparer = owner
	}
	if viewQueryContext() {
		description.Features = append(description.Features, provider.ViewQueryContextFeature)
		engine.Features = append(engine.Features, provider.ViewQueryContextFeature)
	}
	if err := runtime.Serve(server); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type commonObjects struct{ *backend.Provider }

// Open adds the capabilities the comparison reads to compare the objects the
// program carries: views and their comments, roles, their memberships and
// their comments, and whether a view is replaced in place.
func (p *commonObjects) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := p.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	if session.Capabilities == nil {
		session.Capabilities = make(map[string]bool)
	}
	for _, capability := range []string{"views", "view_comments", "role_management", "role_membership", "role_comments"} {
		session.Capabilities[capability] = true
	}
	// FIXTURE_CREATE_OR_REPLACE_VIEW=true reports that the engine replaces a
	// view in place, keeping its grants; without it a changed view is
	// dropped and created again, as on an engine that has no such statement.
	session.Capabilities["create_or_replace_view"] = os.Getenv("FIXTURE_CREATE_OR_REPLACE_VIEW") == "true"
	if materializedViews() {
		session.Capabilities["materialized_views"] = true
	}
	return session, nil
}
