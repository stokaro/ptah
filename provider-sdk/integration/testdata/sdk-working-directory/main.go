// Package main reports the directory it runs in. It serves the fixture, and
// a session it opens names that directory as its database, so a process test
// sees where the bootstrap started the provider.
package main

import (
	"context"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-sdk-working-directory"
	owner := &directoryOwner{Provider: &backend.Provider{}}
	if err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type directoryOwner struct {
	*backend.Provider
}

// Open opens the fixture database the address names, and names the
// provider's working directory as the session's database.
func (o *directoryOwner) Open(ctx context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	session, err := o.Provider.Open(ctx, request)
	if err != nil {
		return nil, err
	}
	session.Database, err = os.Getwd()
	return session, err
}
