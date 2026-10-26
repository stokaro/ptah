// Package main serves the fixture with the common-object wire of contract
// 25.3: views, roles and groups, grants and comments.
//
// SQLite has views and nothing else of the four, so the program keeps roles,
// grants and comments in a file of its own beside the database, which its
// reads report and its executor writes. Each change renders as one statement
// its own executor reads: `FIXTURE {...};`, the change as JSON. A view is a
// real SQLite view; the executor creates it with CREATE VIEW.
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

// commonKinds are the kinds the program carries.
var commonKinds = []protocol.CommonObjectSupport_Kind{
	protocol.CommonObjectSupport_VIEW, protocol.CommonObjectSupport_ROLE, protocol.CommonObjectSupport_GRANT, protocol.CommonObjectSupport_COMMENT,
}

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
	engine.CommonObjects = &protocol.CommonObjectSupport{Kinds: commonKinds}
	owner := &commonObjects{Provider: &backend.Provider{ViewsElsewhere: true}}
	if err := runtime.Serve(runtime.Server{
		Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, Locks: owner, SQLAnalyzer: owner,
		ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner, CommonObjects: owner,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type commonObjects struct{ *backend.Provider }

// Open adds the capabilities the comparison reads to compare the objects the
// program carries: views and their comments, roles, their memberships and
// their comments.
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
	return session, nil
}
