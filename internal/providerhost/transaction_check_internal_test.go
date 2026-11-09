package providerhost

// White-box testing required: a session whose target has no client shows that
// the answer asks the provider nothing, and that a dry run keeps answering for
// the run that follows it. Through the public surface both need a provider that
// answers, which would hide a call made too early.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// transactionSession is a session of an engine that lists the features and
// whose opened session states the transactional DDL capability as given.
func transactionSession(features []string, transactionalDDL bool) *session {
	return &session{
		target: &target{
			engine: &protocol.Engine{Id: "fixture", Features: features},
			host:   &Host{description: &protocol.Description{Features: features}},
		},
		info: catalog.ServerInfo{Capabilities: capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: transactionalDDL})},
	}
}

func TestSessionCheckTransaction_HappyPath(t *testing.T) {
	c := qt.New(t)
	s := transactionSession([]string{provider.TransactionsFeature}, true)

	c.Assert(s.CheckTransaction(), qt.IsNil)
}

func TestSessionCheckTransaction_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name             string
		features         []string
		transactionalDDL bool
		dryRun           bool
		wantErr          string
	}{
		{
			name:             "no transaction service",
			transactionalDDL: true,
			wantErr:          "unsupported feature: the experimental provider contract does not support transactions for the selected engine",
		},
		{
			name:     "a service without established transactional DDL",
			features: []string{provider.TransactionsFeature},
			wantErr:  "unsupported feature: the experimental provider contract does not support transactions without established transactional DDL",
		},
		{
			name:             "a dry run answers for the run that follows it",
			transactionalDDL: true,
			dryRun:           true,
			wantErr:          "unsupported feature: the experimental provider contract does not support transactions for the selected engine",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			s := transactionSession(test.features, test.transactionalDDL)
			s.dry.Store(test.dryRun)

			err := s.CheckTransaction()

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// BeginTransaction refuses for the reasons CheckTransaction gives, and a dry run
// begins nothing, so it has nothing to refuse.
func TestSessionBeginTransaction_AgreesWithTheCheck(t *testing.T) {
	c := qt.New(t)
	s := transactionSession(nil, true)

	tx, err := s.BeginTransaction(c.Context())
	c.Assert(err, qt.ErrorMatches, s.CheckTransaction().Error())
	c.Assert(tx, qt.IsNil)

	s.SetDryRun(true)
	dry, err := s.BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(dry.IsDryRun(), qt.IsTrue)
}
