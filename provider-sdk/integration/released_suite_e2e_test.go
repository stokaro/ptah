//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"ptah.run/provider-sdk/conformance"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// suiteSDK is the SDK release whose conformance suite checks a provider of the
// in-tree SDK, and whose fixture provider the in-tree suite checks. It is the
// oldest release that ships the suite, so the two sides of each run are as many
// minors apart as the suite can measure. When the contract freezes as v1 it
// names the first v1 release and stays there (ADR 0102).
const suiteSDK = "v0.26.0"

// suiteContract is the contract suiteSDK speaks, the one its suite reports.
const suiteContract = "25.28"

// conformanceCommand is the import path of the suite's command.
const conformanceCommand = "github.com/stokaro/ptah/provider-sdk/cmd/ptah-provider-conformance"

// buildReleasedSuite installs the conformance command of an SDK release into a
// directory of its own. `go install` with a version builds the release's module
// as published, outside any workspace, so nothing of the in-tree SDK reaches
// it. GOFLAGS is cleared so that a flag set for the in-tree module, such as
// -modfile, does not reach a build that has no module of its own.
func buildReleasedSuite(c *qt.C, version string) string {
	c.Helper()
	bin := c.TempDir()
	install := exec.CommandContext(c.Context(), "go", "install", conformanceCommand+"@"+version) // #nosec G204 -- the command and the version are constants of this file
	install.Dir = c.TempDir()
	install.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=", "GOBIN="+bin)
	output, err := install.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go install: %s", output))
	entries, err := os.ReadDir(bin)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
	binary := filepath.Join(bin, entries[0].Name())
	assertLinkedSDK(c, install.Dir, binary, version)
	return binary
}

// suiteFixtureFile is the command's fixture file, as far as these runs fill it.
type suiteFixtureFile struct {
	Calls []suiteFixtureCall `json:"calls"`
	Live  struct {
		Changes []suiteFixtureChange `json:"changes"`
	} `json:"live"`
}

type suiteFixtureCall struct {
	Name    string          `json:"name"`
	Method  string          `json:"method"`
	Request json.RawMessage `json:"request"`
	Refused bool            `json:"refused"`
}

type suiteFixtureChange struct {
	Name   string           `json:"name"`
	Apply  suiteFixtureStep `json:"apply"`
	Revert suiteFixtureStep `json:"revert"`
}

type suiteFixtureStep struct {
	Render json.RawMessage `json:"render"`
}

// writeSuiteFixtures writes the calls and changes of the fixture provider's
// conformance runs as the command's fixture file. Each request is in the
// Protobuf JSON mapping, which is how a suite of another release reads what
// this SDK wrote, so the file also shows that the mapping of these requests
// did not move.
func writeSuiteFixtures(c *qt.C) string {
	c.Helper()
	methods := make(map[protoreflect.FullName]string)
	service := protocol.File_protocol_experimental_provider_proto.Services().ByName("Provider")
	for i := range service.Methods().Len() {
		method := service.Methods().Get(i)
		methods[method.Input().FullName()] = string(method.Name())
	}
	encode := func(message proto.Message) json.RawMessage {
		data, err := protojson.Marshal(message)
		c.Assert(err, qt.IsNil)
		return data
	}
	var file suiteFixtureFile
	for _, call := range fixtureCalls() {
		method, ok := methods[call.Request.ProtoReflect().Descriptor().FullName()]
		c.Assert(ok, qt.IsTrue, qt.Commentf("no method takes the request of call %s", call.Name))
		file.Calls = append(file.Calls, suiteFixtureCall{Name: call.Name, Method: method, Request: encode(call.Request), Refused: call.Refused})
	}
	for _, change := range fixtureChanges() {
		file.Live.Changes = append(file.Live.Changes, suiteFixtureChange{Name: change.Name,
			Apply: suiteFixtureStep{Render: encode(change.Apply.Render)}, Revert: suiteFixtureStep{Render: encode(change.Revert.Render)}})
	}
	data, err := json.Marshal(file)
	c.Assert(err, qt.IsNil)
	path := filepath.Join(c.TempDir(), "fixtures.json")
	c.Assert(os.WriteFile(path, data, 0o600), qt.IsNil)
	return path
}

// statuses maps each check of a report to its outcome.
func statuses(report *conformance.Report) map[string]conformance.Status {
	outcomes := make(map[string]conformance.Status, len(report.Results))
	for _, result := range report.Results {
		outcomes[result.ID] = result.Status
	}
	return outcomes
}

// exercisedChecks are checks a run against the fixture provider must pass
// rather than report as not applicable: the offline calls and the live change
// came from the configuration and reached the provider.
func exercisedChecks() []string {
	return []string{"bootstrap.start", "negotiation.description", "calls.render-create", "calls.render-nullable-key", "live.open", "live.inspect", "live.changes.items"}
}

// The suite of an earlier release acts as a strict host of that release. The
// fixture provider built with the in-tree SDK passes it, offline and against a
// database file: nothing the in-tree SDK adds breaks a host of an earlier
// minor.
func TestReleasedSuiteChecksAProviderOfThisSDK(t *testing.T) {
	c := qt.New(t)
	suite := buildReleasedSuite(c, suiteSDK)
	binary := buildFixture(c, "provider")
	run := exec.CommandContext(c.Context(), suite, "-json", "-executable", binary, "-provider-id", "example.org/fixture", // #nosec G204 -- the test built both executables
		"-require", strings.Join(fixtureRequiredFeatures(), ","), "-fixtures", writeSuiteFixtures(c))
	run.Env = append(os.Environ(), "PTAH_CONFORMANCE_ADDRESS="+fixtureDatabase(c))

	stdout, err := run.Output()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stdout))
	var report conformance.Report
	c.Assert(json.Unmarshal(stdout, &report), qt.IsNil)
	c.Assert(report.Failures(), qt.HasLen, 0)
	c.Assert(report.Contract, qt.Equals, suiteContract)
	outcomes := statuses(&report)
	for _, id := range exercisedChecks() {
		c.Assert(outcomes[id], qt.Equals, conformance.Passed, qt.Commentf("check %s", id))
	}
}

// The in-tree suite acts as a strict host of the in-tree contract against the
// fixture provider built with an earlier release: a host of a later minor
// serves a provider of an earlier one.
func TestThisSuiteChecksAProviderOfTheReleasedSDK(t *testing.T) {
	c := qt.New(t)
	binary := buildReleasedFixture(c, suiteSDK, "provider")

	report, err := conformance.Check(c.Context(), fixtureConformance(c, binary))

	c.Assert(err, qt.IsNil)
	c.Assert(report.Err(), qt.IsNil)
	c.Assert(report.Contract, qt.Equals, provider.ContractString(provider.Contract()))
	c.Assert(report.Provider, qt.IsNotNil)
	c.Assert(report.Provider.MinContract, qt.Equals, "25.0")
	outcomes := statuses(report)
	for _, id := range exercisedChecks() {
		c.Assert(outcomes[id], qt.Equals, conformance.Passed, qt.Commentf("check %s", id))
	}
}
