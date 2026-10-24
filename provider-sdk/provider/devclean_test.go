package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func devCleanupDescription() *protocol.Description {
	description := devDescription()
	description.Features = append(description.Features, provider.DevCleanupFeature, provider.ReplayGuardFeature)
	engine := description.Engines[0]
	engine.Features = append(engine.Features, provider.DevCleanupFeature, provider.ReplayGuardFeature, provider.DevRealmsFeature)
	return description
}

func devObject(kind, name, refusal string) *protocol.DevObject {
	return &protocol.DevObject{Object: &protocol.Reference{Kind: kind, Name: name}, Refusal: refusal}
}

func TestDevCleanupDescription_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateDescription(devCleanupDescription(), nil), qt.IsNil)
}

func TestDevCleanupDescription_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"engine cleanup without the service", func(d *protocol.Description) { d.Features = d.Features[:len(d.Features)-2] }},
		{"engine replay guard without the service", func(d *protocol.Description) { d.Features = d.Features[:len(d.Features)-1] }},
		{"dev realms without provisioning", func(d *protocol.Description) {
			d.Engines[0].Features = []string{provider.DevRealmsFeature}
		}},
		{"dev realms as a service", func(d *protocol.Description) { d.Features = append(d.Features, provider.DevRealmsFeature) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := devCleanupDescription()
			test.mutate(description)
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateDevObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := &protocol.ListDevObjectsRequest{SessionId: "session", Schemas: []string{"app"}}
	c.Assert(provider.ValidateDevObjects(request, &protocol.DevObjects{Complete: true}), qt.IsNil)
	listed := &protocol.DevObjects{Complete: true, Objects: []*protocol.DevObject{
		devObject("table", "rates", ""), devObject("column-store", "events", "no statement drops a column store"),
	}}
	c.Assert(provider.ValidateDevObjects(request, listed), qt.IsNil)
}

func TestValidateDevObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request *protocol.ListDevObjectsRequest
		result  *protocol.DevObjects
		want    error
	}{
		{"no session", &protocol.ListDevObjectsRequest{}, &protocol.DevObjects{Complete: true}, provider.ErrProtocol},
		{"repeated schema", &protocol.ListDevObjectsRequest{SessionId: "s", Schemas: []string{"app", "app"}}, &protocol.DevObjects{Complete: true}, provider.ErrProtocol},
		{"blank schema", &protocol.ListDevObjectsRequest{SessionId: "s", Schemas: []string{" "}}, &protocol.DevObjects{Complete: true}, provider.ErrProtocol},
		{"missing reply", &protocol.ListDevObjectsRequest{SessionId: "s"}, nil, provider.ErrIncomplete},
		{"incomplete list", &protocol.ListDevObjectsRequest{SessionId: "s"}, &protocol.DevObjects{}, provider.ErrIncomplete},
		{"object without a kind", &protocol.ListDevObjectsRequest{SessionId: "s"},
			&protocol.DevObjects{Complete: true, Objects: []*protocol.DevObject{devObject("", "rates", "")}}, provider.ErrProtocol},
		{"object without a name", &protocol.ListDevObjectsRequest{SessionId: "s"},
			&protocol.DevObjects{Complete: true, Objects: []*protocol.DevObject{devObject("table", "", "")}}, provider.ErrProtocol},
		{"object listed twice", &protocol.ListDevObjectsRequest{SessionId: "s"},
			&protocol.DevObjects{Complete: true, Objects: []*protocol.DevObject{devObject("table", "rates", ""), devObject("table", "rates", "")}}, provider.ErrProtocol},
		{"refusal of two lines", &protocol.ListDevObjectsRequest{SessionId: "s"},
			&protocol.DevObjects{Complete: true, Objects: []*protocol.DevObject{devObject("table", "rates", "a\nb")}}, provider.ErrProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDevObjects(test.request, test.result), qt.ErrorIs, test.want)
		})
	}
}

func cleanDevRequest() *protocol.CleanDevRequest {
	return &protocol.CleanDevRequest{SessionId: "session", RequestId: "clean-1"}
}

func TestCheckDevCleanup_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.CheckDevCleanup(cleanDevRequest(), &protocol.DevCleanup{RequestId: "clean-1", Outcome: protocol.DevCleanup_CLEANED}), qt.IsNil)
}

func TestCheckDevCleanup_FailurePath(t *testing.T) {
	blocking := []*protocol.DevObject{devObject("column-store", "events", "no statement drops a column store")}
	tests := []struct {
		name   string
		result *protocol.DevCleanup
		want   error
	}{
		{"refused", &protocol.DevCleanup{RequestId: "clean-1", Outcome: protocol.DevCleanup_REFUSED, Blocking: blocking}, provider.ErrExecution},
		{"unknown", &protocol.DevCleanup{RequestId: "clean-1", Message: "dropped 3 of 5 objects"}, provider.ErrOutcomeUnknown},
		{"another request", &protocol.DevCleanup{RequestId: "clean-2", Outcome: protocol.DevCleanup_CLEANED}, provider.ErrProtocol},
		{"missing reply", nil, provider.ErrOutcomeUnknown},
		{"unknown outcome", &protocol.DevCleanup{RequestId: "clean-1", Outcome: 9}, provider.ErrProtocol},
		{"refused naming nothing", &protocol.DevCleanup{RequestId: "clean-1", Outcome: protocol.DevCleanup_REFUSED}, provider.ErrProtocol},
		{"refused without a reason", &protocol.DevCleanup{RequestId: "clean-1", Outcome: protocol.DevCleanup_REFUSED,
			Blocking: []*protocol.DevObject{devObject("table", "rates", "")}}, provider.ErrProtocol},
		{"cleaned naming an object", &protocol.DevCleanup{RequestId: "clean-1", Outcome: protocol.DevCleanup_CLEANED, Blocking: blocking}, provider.ErrProtocol},
		{"a message of two lines", &protocol.DevCleanup{RequestId: "clean-1", Outcome: protocol.DevCleanup_CLEANED, Message: "a\nb"}, provider.ErrProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.CheckDevCleanup(cleanDevRequest(), test.result), qt.ErrorIs, test.want)
		})
	}
	c := qt.New(t)
	c.Assert(provider.ValidateCleanDevRequest(&protocol.CleanDevRequest{SessionId: "session"}), qt.ErrorIs, provider.ErrProtocol)
}

func guardRequest() *protocol.GuardReplayRequest {
	return &protocol.GuardReplayRequest{Engine: devEngineID(), Scope: &protocol.ReplayScope{Database: "local"},
		Statements: []string{"CREATE TABLE a (id Int32, PRIMARY KEY (id))", "CREATE USER u"}}
}

func TestValidateGuardedReplay_HappyPath(t *testing.T) {
	c := qt.New(t)
	result := &protocol.GuardedReplay{Verdicts: []*protocol.ReplayVerdict{{}, {Refused: true, Operation: "CREATE USER"}}}
	c.Assert(provider.ValidateGuardedReplay(devCleanupDescription(), guardRequest(), result), qt.IsNil)
	for _, scope := range []*protocol.ReplayScope{nil, {Server: true, Provisioned: true}, {Databases: true, Session: true}} {
		request := guardRequest()
		request.Scope = scope
		c.Assert(provider.ValidateGuardedReplay(devCleanupDescription(), request, result), qt.IsNil)
	}
}

func TestValidateGuardReplayRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.GuardReplayRequest)
	}{
		{"no service", func(d *protocol.Description, _ *protocol.GuardReplayRequest) {
			d.Features = d.Features[:len(d.Features)-1]
		}},
		{"engine without the service", func(d *protocol.Description, _ *protocol.GuardReplayRequest) {
			d.Engines[0].Features = []string{provider.DevDatabaseFeature, provider.DevCleanupFeature}
		}},
		{"provisioned without the server", func(_ *protocol.Description, r *protocol.GuardReplayRequest) { r.Scope.Provisioned = true }},
		{"the server's databases and the server", func(_ *protocol.Description, r *protocol.GuardReplayRequest) {
			r.Scope.Server, r.Scope.Databases = true, true
		}},
		{"a blank statement", func(_ *protocol.Description, r *protocol.GuardReplayRequest) { r.Statements[0] = " \n" }},
		{"a statement that is not text", func(_ *protocol.Description, r *protocol.GuardReplayRequest) { r.Statements[0] = "\xff" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request := devCleanupDescription(), guardRequest()
			test.mutate(description, request)
			c.Assert(provider.ValidateGuardReplayRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateGuardedReplay_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		verdicts []*protocol.ReplayVerdict
	}{
		{"a verdict missing", []*protocol.ReplayVerdict{{}}},
		{"a refusal naming nothing", []*protocol.ReplayVerdict{{}, {Refused: true}}},
		{"a refusal of two lines", []*protocol.ReplayVerdict{{}, {Refused: true, Operation: "CREATE\nUSER"}}},
		{"an accepted statement naming an operation", []*protocol.ReplayVerdict{{Operation: "CREATE TABLE"}, {Refused: true, Operation: "CREATE USER"}}},
		{"an accepted statement naming two lines", []*protocol.ReplayVerdict{{Operation: "a\nb"}, {Refused: true, Operation: "CREATE USER"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := &protocol.GuardedReplay{Verdicts: test.verdicts}
			c.Assert(provider.ValidateGuardedReplay(devCleanupDescription(), guardRequest(), result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}
