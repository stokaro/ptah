package conformance_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/conformance"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// hostRequires is what Ptah requires of every provider.
var hostRequires = []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature}

func notesTable() *protocol.Reference { return &protocol.Reference{Kind: "table", Name: "notes"} }

func createNotes() *protocol.Change {
	return &protocol.Change{Id: "create", Object: notesTable(), Operation: &protocol.Change_CreateTable{
		CreateTable: &protocol.Table{Columns: []*protocol.Column{{Name: "id", Type: "INTEGER", Nullable: true}}},
	}}
}

func dropNotes() *protocol.Change {
	return &protocol.Change{Id: "drop", Object: notesTable(), Operation: &protocol.Change_DropTable{DropTable: &protocol.Empty{}}}
}

// memoryConfig gives the suite something for every check: a call, a change,
// a refused unit and a credential address, over the database name names.
func memoryConfig(target conformance.Target, name string) conformance.Config {
	return conformance.Config{
		Target:            target,
		ProviderID:        memoryProviderID,
		RequiredFeatures:  hostRequires,
		CredentialAddress: "memory://user:" + conformance.Canary + "@nowhere/" + name,
		Calls: []conformance.Call{
			{Name: "render-create", Request: &protocol.RenderRequest{Engine: memoryEngine, Changes: []*protocol.Change{createNotes()}}},
			{Name: "render-index", Request: &protocol.RenderRequest{Engine: memoryEngine, Changes: []*protocol.Change{{
				Id: "column", Object: notesTable(), Operation: &protocol.Change_AddColumn{AddColumn: &protocol.Column{Name: "body", Type: "TEXT", Nullable: true}},
			}}}, Refused: true},
		},
		Live: &conformance.Live{
			Address: "memory:///" + name,
			Changes: []conformance.Change{{
				Name:   "notes",
				Apply:  conformance.Step{Render: &protocol.RenderRequest{Changes: []*protocol.Change{createNotes()}}},
				Revert: conformance.Step{Render: &protocol.RenderRequest{Changes: []*protocol.Change{dropNotes()}}},
			}},
			Refused: []*protocol.Unit{{ChangeId: "missing", Body: &protocol.Unit_Sql{Sql: "DROP TABLE missing"}}},
		},
	}
}

// statuses maps each check of a report to its outcome.
func statuses(report *conformance.Report) map[string]conformance.Status {
	found := make(map[string]conformance.Status, len(report.Results))
	for _, result := range report.Results {
		found[result.ID] = result.Status
	}
	return found
}

// failed returns the checks of a report that failed, sorted.
func failed(report *conformance.Report) []string {
	var ids []string
	for _, result := range report.Failures() {
		ids = append(ids, result.ID)
	}
	slices.Sort(ids)
	return ids
}

// conforming is the outcome of every check for the memory provider under
// memoryConfig: every check with something to check passes.
func conforming() map[string]conformance.Status {
	return map[string]conformance.Status{
		"bootstrap.start":              conformance.Passed,
		"handshake.other-major":        conformance.Passed,
		"handshake.contract-major":     conformance.Passed,
		"handshake.host-features":      conformance.Passed,
		"handshake.required-features":  conformance.Passed,
		"handshake.contract-25.0-host": conformance.Passed,
		"handshake.minimum-contract":   conformance.Passed,
		"description.valid":            conformance.Passed,
		"description.identity":         conformance.Passed,
		"description.stable":           conformance.Passed,
		"negotiation.description":      conformance.Passed,
		"services.advertised":          conformance.Passed,
		"services.unadvertised":        conformance.Passed,
		"sessions.unknown":             conformance.Passed,
		"database-context.credentials": conformance.NotApplicable,
		"models.recorded":              conformance.NotApplicable,
		"calls.render-create":          conformance.Passed,
		"calls.render-index":           conformance.Passed,
		"live.open":                    conformance.Passed,
		"live.close":                   conformance.Passed,
		"live.sessions":                conformance.Passed,
		"live.restart":                 conformance.NotApplicable,
		"live.inspect":                 conformance.Passed,
		"live.locks":                   conformance.Passed,
		"live.transactions":            conformance.Passed,
		"live.credentials":             conformance.Passed,
		"live.changes.notes":           conformance.Passed,
		"live.refused":                 conformance.Passed,
		"live.calls":                   conformance.NotApplicable,
		"reasons.negotiated":           conformance.Passed,
		"reasons.redacted":             conformance.Passed,
	}
}

// assertConforms asserts a report of the memory provider under memoryConfig
// in which nothing failed.
func assertConforms(c *qt.C, report *conformance.Report) {
	c.Helper()
	c.Assert(report.Err(), qt.IsNil)
	c.Assert(report.Provider.ID, qt.Equals, memoryProviderID)
	c.Assert(report.Provider.Engines, qt.DeepEquals, []string{memoryEngine})
	c.Assert(report.Contract, qt.Equals, provider.ContractString(provider.Contract()))
	c.Assert(statuses(report), qt.DeepEquals, conforming())
}

func TestCheck_HappyPath(t *testing.T) {
	t.Run("in process", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.InProcess(newMemoryProvider(memoryDefects{}).server())

		report, err := conformance.Check(c.Context(), memoryConfig(target, "happy"))

		c.Assert(err, qt.IsNil)
		assertConforms(c, report)
		c.Assert(report.Target, qt.Equals, "in-process")
	})
	t.Run("a wire service", func(t *testing.T) {
		c := qt.New(t)
		target := conformance.Wire(wireTarget(c, newMemoryProvider(memoryDefects{}), wireDefects{}))

		report, err := conformance.Check(c.Context(), memoryConfig(target, "happy"))

		c.Assert(err, qt.IsNil)
		assertConforms(c, report)
		c.Assert(report.Target, qt.Equals, "wire")
	})
}

// Each row breaks the memory provider in one way, and the suite fails
// exactly the checks that exist to find that defect. Without the defect,
// TestCheck_HappyPath passes every one of them.
func TestCheck_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		defects memoryDefects
		want    []string
	}{
		{
			name:    "a render reply the validator refuses",
			defects: memoryDefects{renderDropsUnit: true},
			want:    []string{"calls.render-create", "live.changes.notes", "live.refused"},
		},
		{
			name:    "a render that depends on more than its request",
			defects: memoryDefects{renderCounts: true},
			want:    []string{"calls.render-create"},
		},
		{
			name:    "a reason that quotes the address",
			defects: memoryDefects{leakAddress: true},
			want:    []string{"live.credentials", "reasons.redacted"},
		},
		{
			name:    "a reply field of a feature the host did not list",
			defects: memoryDefects{unreadRegardless: true},
			want:    []string{"live.inspect"},
		},
		{
			name:    "a lock two sessions hold",
			defects: memoryDefects{sharedLocks: true},
			want:    []string{"live.locks"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := conformance.InProcess(newMemoryProvider(test.defects).server())

			report, err := conformance.Check(c.Context(), memoryConfig(target, "broken"))

			c.Assert(err, qt.IsNil)
			c.Assert(failed(report), qt.DeepEquals, test.want, qt.Commentf("%v", report.Err()))
			c.Assert(report.Err(), qt.ErrorIs, conformance.ErrNonconforming)
		})
	}
}

// A provider outside the SDK can send what the SDK's server would not let
// one send. Each row breaks the wire service in one way.
func TestCheck_WireFailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		defects wireDefects
		want    []string
	}{
		{
			name:    "a description that ignores the host's features",
			defects: wireDefects{describeAll: true},
			want:    []string{"handshake.contract-25.0-host", "handshake.host-features", "live.inspect", "negotiation.description", "reasons.negotiated"},
		},
		{
			name:    "a reason the host did not negotiate",
			defects: wireDefects{reasonAlways: true},
			want:    []string{"reasons.negotiated"},
		},
		{
			name:    "an offered service that answers Unimplemented",
			defects: wireDefects{renderUnimplemented: true},
			want:    []string{"calls.render-create", "live.changes.notes", "live.refused", "services.advertised"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := conformance.Wire(wireTarget(c, newMemoryProvider(memoryDefects{}), test.defects))

			report, err := conformance.Check(c.Context(), memoryConfig(target, "broken"))

			c.Assert(err, qt.IsNil)
			c.Assert(failed(report), qt.DeepEquals, test.want, qt.Commentf("%v", report.Err()))
		})
	}
}
