// Package main exercises native object namespaces and refused placements through
// an independent process. Fixture table names select the reply shape; this is
// transport evidence and does not implement a database feature.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	description := backend.Description()
	description.SemanticIdentity += "-namespaced-yaml"
	owner := &namespacedProvider{Provider: &backend.Provider{}}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: owner, Executor: owner, SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type namespacedProvider struct{ *backend.Provider }

func (p *namespacedProvider) DecodeSource(ctx context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	result, err := p.Provider.DecodeSource(ctx, request)
	if err != nil {
		return nil, err
	}
	for _, document := range result.GetDocuments() {
		document.Coverage.Subjects = nil
		for _, fragment := range document.GetFragments() {
			original := fragment.Values[0]
			fragment.Values = nil
			for i, scope := range []struct {
				database string
				path     []string
			}{{"tenant", []string{"a/b", "c"}}, {"tenant", []string{"a", "b/c"}}, {"other", []string{"a/b", "c"}}} {
				value := namespaceValue(original, scope.database, scope.path)
				value.Value.Id += "-" + strconv.Itoa(i)
				fragment.Values = append(fragment.Values, value)
				document.Coverage.Subjects = append(document.Coverage.Subjects, &protocol.ModelSubjectCoverage{
					Kind: value.GetValue().GetValue().GetKind(), Subject: proto.CloneOf(value.GetValue().GetValue().GetOwner()),
					NormalizedSubject: proto.CloneOf(value.GetValue().GetNormalizedOwner()), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE},
				})
			}
		}
	}
	return result, nil
}

func namespaceValue(original *protocol.SourceModelValue, database string, path []string) *protocol.SourceModelValue {
	value := proto.CloneOf(original)
	ref := value.Value.Value.Owner
	ref.Database, ref.Path = database, path
	if ref.GetName() != "facet" {
		value.Placement = protocol.SourceModelValue_NATIVE_OBJECT
		ref.Kind = value.Value.Value.Kind
	}
	if ref.GetName() == "overload" {
		ref.Overload = []string{"integer"}
	}
	value.Value.NormalizedOwner = proto.CloneOf(ref)
	if ref.GetName() == "reshape" {
		value.Value.NormalizedOwner.Path = append(value.Value.NormalizedOwner.Path, "extra")
	}
	return value
}
