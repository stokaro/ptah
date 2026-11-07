// Package main runs a fixture provider whose engine has no string type with a
// length: its render writes a VARCHAR(n) column as TEXT and, under
// render.omissions, reports the length it left out, as a YDB render reports
// the length a Utf8 column does not keep.
//
// FIXTURE_RENDER_OMISSIONS=off stands for a provider that predates the
// feature: it lists none, and its render drops the length without a word.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func main() {
	owner := &backend.Provider{}
	description := backend.Description()
	description.SemanticIdentity = "fixture-render-omissions-1"
	if os.Getenv("FIXTURE_RENDER_OMISSIONS") != "off" {
		description.Features = append(description.Features, provider.RenderOmissionsFeature)
		description.Engines[0].Features = append(description.Engines[0].Features, provider.RenderOmissionsFeature)
	}
	err := runtime.Serve(runtime.Server{Description: description, Sessions: owner, Inspector: owner, Renderer: omittingRenderer{owner: owner}, Executor: owner,
		SQLAnalyzer: owner, ValueNormalizer: owner, SourceDecoder: owner, ValueConverter: owner})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// omittingRenderer renders each change as the base fixture does, with every
// VARCHAR(n) column of a created table written as TEXT.
type omittingRenderer struct{ owner *backend.Provider }

// Render reports each length it leaves out when the request's target lists
// render.omissions, in the order of the changes and their columns.
func (r omittingRenderer) Render(ctx context.Context, request *protocol.RenderRequest) (*protocol.RenderResult, error) {
	forwarded := proto.CloneOf(request)
	var omissions []*protocol.RenderOmission
	for _, change := range forwarded.GetChanges() {
		for _, column := range change.GetCreateTable().GetColumns() {
			length, found := strings.CutPrefix(strings.ToUpper(column.GetType()), "VARCHAR(")
			if !found {
				continue
			}
			omissions = append(omissions, &protocol.RenderOmission{Reason: "unsupported", Kind: "column", Name: change.GetObject().GetName() + "." + column.GetName(),
				Property: "type modifier", Detail: column.GetType() + ": length " + strings.TrimSuffix(length, ")")})
			column.Type = "TEXT"
		}
	}
	result, err := r.owner.Render(ctx, forwarded)
	if err != nil {
		return nil, err
	}
	if slices.Contains(request.GetTargetFeatures(), provider.RenderOmissionsFeature) {
		result.Omissions = omissions
	}
	return result, nil
}
