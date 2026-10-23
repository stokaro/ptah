// Package main runs the fixture with Go directives beyond table scope and a
// source decoder that attaches every value to a part the directive does not
// describe, to show that the host refuses the value instead of placing it.
package main

import (
	"context"
	"fmt"
	"os"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
)

// misplacing decodes as the fixture does, then renames the owner of every value
// it returns for a column or an index.
type misplacing struct{ *backend.Provider }

func (m misplacing) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	decoded, err := m.Provider.DecodeSource(ctx, request)
	if err != nil {
		return nil, err
	}
	for _, document := range decoded.GetDocuments() {
		for _, fragment := range document.GetFragments() {
			for _, value := range fragment.GetValues() {
				rename(value.GetValue().GetValue().GetOwner())
				rename(value.GetValue().GetNormalizedOwner())
			}
		}
		for _, subject := range document.GetCoverage().GetSubjects() {
			rename(subject.GetSubject())
			rename(subject.GetNormalizedSubject())
		}
	}
	return decoded, nil
}

func rename(owner *protocol.Reference) {
	if owner.GetKind() == "column" || owner.GetKind() == "index" {
		owner.Name = "misplaced"
	}
}

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-go-misplaced-subject"
	engine := description.GetEngines()[0]
	engine.GoSourceDirectives = append(engine.GoSourceDirectives, backend.ScopedGoSourceDirectives()...)
	owner := &backend.Provider{}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: misplacing{owner}, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
