//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func TestIndependentProviderBindsContextWithoutOpeningAndPreservesObservationFacts(t *testing.T) {
	c := qt.New(t)
	binary := buildSDKFixture(c, "sdk-value-context")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	features := []string{provider.DatabaseContextFeature, provider.ValueContextFeature, provider.CommonObjectsFeature, provider.CommonViewFacetsFeature, provider.ObservedColumnsFeature, provider.PlanningObjectContextFeature}
	client := start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: features, HostFeatures: features})
	engine := client.Description().Engines[0]
	c.Assert(engine.Aliases, qt.Contains, "fixture-context")
	path := filepath.Join(c.TempDir(), "database.db")
	address := (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/"), RawQuery: url.Values{"source": {"/authored"}}.Encode()}).String()
	request := &protocol.DatabaseContextRequest{Engine: "fixture", Address: address}
	bound, err := client.ResolveDatabaseContext(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(bound.Database, qt.Equals, path)
	c.Assert(bound.SourceDatabase, qt.Equals, "/authored")
	_, err = os.Stat(path)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	input := retentionInput(engine.ValueModels[0].DefinitionSha256, "source", `{"seconds":60}`)
	input.Value.Owner = &protocol.Reference{Engine: "fixture", Kind: engine.ValueModels[0].Kind, Name: "items"}
	batch := &protocol.NormalizeValuesRequest{Engine: "fixture", TargetFeatures: slices.Clone(engine.Features), Values: []*protocol.ValueInput{input}}
	unbound, err := client.NormalizeValues(c.Context(), batch)
	c.Assert(err, qt.IsNil)
	c.Assert(unbound.Values[0].RequiresDatabaseContext, qt.IsTrue)
	c.Assert(unbound.Values[0].RequiredObjects, qt.HasLen, 0)
	input.Value.Context = proto.CloneOf(bound)
	resolved, err := client.NormalizeValues(c.Context(), batch)
	c.Assert(err, qt.IsNil)
	c.Assert(resolved.Values[0].RequiresDatabaseContext, qt.IsFalse)
	c.Assert(resolved.Values[0].RequiredObjects[0].Database.Normalized, qt.Equals, path)
	c.Assert(resolved.Values[0].Value.Owner.Database, qt.Equals, "")
	c.Assert(proto.Equal(resolved.Values[0].Value.Context, bound), qt.IsTrue)
	converted, err := client.ConvertValues(c.Context(), &protocol.ConvertValuesRequest{Engine: "fixture", TargetFeatures: batch.TargetFeatures,
		From: protocol.ValueInput_DESIRED, To: protocol.ValueInput_OBSERVED, Values: resolved.Values})
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(converted.Values[0].Value.Context, bound), qt.IsTrue)
	c.Assert(converted.Values[0].RequiredObjects[0].Database.Normalized, qt.Equals, path)
	subject := &protocol.ComparisonIdentity{Kind: input.Value.Kind, Name: &protocol.ComparisonPart{Source: "items", Normalized: "items"}}
	state := func(rep protocol.ValueInput_Representation, value *protocol.NormalizedValue) *protocol.ComparisonState {
		return &protocol.ComparisonState{Representation: rep, Coverage: &protocol.ComparisonCoverage{}, Records: []*protocol.ComparisonRecord{{Kind: value.Value.Kind, Subject: proto.CloneOf(subject), Value: value}}}
	}
	planned, err := client.PlanFeatures(c.Context(), &protocol.PlanFeaturesRequest{Engine: "fixture", PlanningId: "objects", TargetFeatures: batch.TargetFeatures,
		Identifiers: &protocol.ComparisonIdentifiers{Rules: proto.CloneOf(engine.Identifiers), DefaultSchema: engine.DefaultSchema}, DatabasePath: path,
		ContextKinds: []string{input.Value.Kind}, DesiredObjects: state(protocol.ValueInput_DESIRED, resolved.Values[0]), CurrentObjects: state(protocol.ValueInput_OBSERVED, converted.Values[0]),
		ChangedSubjects: []*protocol.ComparisonIdentity{proto.CloneOf(subject)}})
	c.Assert(err, qt.IsNil)
	c.Assert(planned.Objects, qt.HasLen, 1)
	c.Assert(proto.Equal(planned.Objects[0].Subject, subject), qt.IsTrue)
	c.Assert(planned.Objects[0].Steps, qt.HasLen, 0)
	// The temp directory must outlive the process that holds its database file.
	c.Cleanup(client.Close)
	session, err := client.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture", Address: address})
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(session.Context, bound), qt.IsTrue)
	inspect := &protocol.InspectRequest{SessionId: session.Id}
	snapshot, err := client.Inspect(c.Context(), inspect)
	c.Assert(err, qt.IsNil)
	values, err := provider.InspectionValues(client.Description(), inspect, snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(values.Values, qt.HasLen, 1)
	c.Assert(values.Values[0].Value.Owner.Kind, qt.Equals, "view")
	c.Assert(proto.Equal(values.Values[0].Value.Context, bound), qt.IsTrue)
	observed, err := client.NormalizeValues(c.Context(), values)
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Values[0].RequiredObjects[0].Database.Normalized, qt.Equals, path)
	id, label := snapshot.Tables[0].Columns[0], snapshot.Tables[0].Columns[1]
	c.Assert(id.Type, qt.Equals, "")
	c.Assert(id.Observed.IdentityStart, qt.Equals, "7")
	c.Assert(id.Observed.IdentityIncrement, qt.Equals, "3")
	c.Assert(id.Observed.SequenceRestart, qt.Equals, "42")
	c.Assert(id.Observed.OwnedSequence, qt.Equals, "items_id_seq")
	c.Assert(id.Observed.DefaultExpression, qt.IsNil)
	c.Assert(label.Observed.DefaultExpression, qt.DeepEquals, new(""))
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	_, err = client.ResolveDatabaseContext(canceled, request)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	request.Engine = "foreign"
	_, err = client.ResolveDatabaseContext(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}
