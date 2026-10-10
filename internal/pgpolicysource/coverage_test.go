package pgpolicysource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

var complete = schemaext.Knowledge{State: schemaext.Complete}

// coverageWith is complete coverage of both models with one subject record,
// for kind; other is the other model.
func coverageWith(kind, other schemaext.Kind, subject objectidentity.ID, knowledge schemaext.Knowledge) schemaext.Coverage {
	return must.Must(must.Must(pgpolicy.Coverage(kind, schemaext.Desired, complete,
		[]schemaext.SubjectCoverage{{Kind: kind, Subject: subject, Knowledge: knowledge}})).
		Combine(must.Must(pgpolicy.Coverage(other, schemaext.Desired, complete, nil))))
}

// policyCoverage records knowledge about the policy tenant on orders.
func policyCoverage(knowledge schemaext.Knowledge) schemaext.Coverage {
	return coverageWith(pgpolicy.PolicyKind, pgpolicy.TableStateKind, pgpolicy.PolicyRef("", "orders", "tenant"), knowledge)
}

// switchCoverage records knowledge about the switches of orders.
func switchCoverage(knowledge schemaext.Knowledge) schemaext.Coverage {
	return coverageWith(pgpolicy.TableStateKind, pgpolicy.PolicyKind, pgpolicy.Table(pgpolicy.PolicyRef("", "orders", "x")), knowledge)
}

func policyOn(table string) schemaext.Objects {
	return must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", table, "tenant"), pgpolicy.DesiredPolicy{}))))
}

func TestRequireRepresentable_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		coverage func() schemaext.Coverage
		table    string
	}{
		{name: "complete coverage", table: "orders", coverage: func() schemaext.Coverage {
			return must.Must(pgpolicy.CompleteCoverage(schemaext.Desired))
		}},
		{name: "an absent policy", table: "orders", coverage: func() schemaext.Coverage {
			return policyCoverage(schemaext.Knowledge{State: schemaext.Absent})
		}},
		{name: "switches left unmanaged beside the table's policies", table: "orders", coverage: func() schemaext.Coverage {
			return switchCoverage(pgpolicysource.UnmanagedSwitches())
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgpolicysource.RequireRepresentable(test.coverage(), policyOn(test.table)), qt.IsNil)
		})
	}
}

func TestRequireRepresentable_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		coverage func() schemaext.Coverage
		table    string
	}{
		{name: "a model the source did not describe", table: "orders", coverage: func() schemaext.Coverage {
			return must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))
		}},
		{name: "a policy the source could not describe", table: "orders", coverage: func() schemaext.Coverage {
			return policyCoverage(schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "partial"})
		}},
		{name: "switches left unmanaged on a table with no policy", table: "invoices", coverage: func() schemaext.Coverage {
			return switchCoverage(pgpolicysource.UnmanagedSwitches())
		}},
		{name: "switches not read for another reason", table: "orders", coverage: func() schemaext.Coverage {
			return switchCoverage(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "no access"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgpolicysource.RequireRepresentable(test.coverage(), policyOn(test.table)), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}
