package provider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// A reply validator checks the reply alone: the request is the request
// validator's, and the SDK's client and server ask that once per call. The
// full validator still checks both.
func TestReplyValidators_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, request, result := planningExchange()
	request.CommonSteps[0].Metadata = nil

	c.Assert(provider.ValidatePlanningRequest(d, request), qt.ErrorMatches, `.*missing planning metadata`)
	c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorMatches, `.*missing planning metadata`)
	c.Assert(provider.ValidatePlannedFeaturesReply(d, request, result), qt.IsNil)
}

// Every defect of a reply the full validator refuses, the reply validator
// refuses in the same words.
func TestReplyValidators_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*protocol.PlannedFeatures)
		wantErr string
	}{
		{name: "an incomplete reply", wantErr: `.*incomplete feature planning`,
			mutate: func(r *protocol.PlannedFeatures) { r.Complete = false }},
		{name: "a missing change receipt", wantErr: `.*missing planning change receipts`,
			mutate: func(r *protocol.PlannedFeatures) { r.Changes = nil }},
		{name: "a missing parent receipt", wantErr: `.*missing planning parent receipt`,
			mutate: func(r *protocol.PlannedFeatures) { r.Parents = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, result := planningExchange()
			test.mutate(result)

			full := provider.ValidatePlannedFeatures(d, request, result)
			alone := provider.ValidatePlannedFeaturesReply(d, request, result)

			c.Assert(alone, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(alone, qt.ErrorMatches, test.wantErr)
			c.Assert(full, qt.ErrorMatches, test.wantErr)
		})
	}
}
