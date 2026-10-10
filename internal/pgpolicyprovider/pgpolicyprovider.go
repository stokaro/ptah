// Package pgpolicyprovider assembles the PostgreSQL row-security owner of
// package pgpolicy as one engine provider: its codecs, and its conversion,
// comparison, normalization, planning and reversal services on every
// PostgreSQL-family target that has row security.
//
// The bundled runtime does not select it yet. It registers only the change and
// operation codecs: a registered desired model is one the schema census
// measures through a render, and a new table's policies have no place in a
// plan or a render until the host passes a table's creation to its owners.
// Tests select this provider through engine.New to drive the services the way
// the runtime will.
package pgpolicyprovider

import (
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policycompare"
	"ptah.run/feature/pgpolicy/policyconvert"
	"ptah.run/feature/pgpolicy/policyplan"
	"ptah.run/feature/pgpolicy/policyprobe"
	"ptah.run/feature/pgpolicy/policyreverse"
)

// Targets are the targets the services are registered for. Spanner speaks the
// PostgreSQL dialect without row security, so it has no row-security state to
// compare.
func Targets() []string {
	return []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB}
}

// Provider returns a fresh descriptor of the owner. The runtime that selects
// it must also select a provider of each of [Targets].
func Provider() engine.Provider {
	provider := engine.Provider{
		ID:     pgpolicy.Owner,
		Codecs: slices.Concat(pgpolicy.Codecs(), pgpolicy.ChangeCodecs(), pgpolicy.OperationCodecs()),
	}
	models := []schemaext.Kind{pgpolicy.PolicyKind, pgpolicy.TableStateKind}
	changes := []schemaext.Kind{pgpolicy.PolicyChangeKind, pgpolicy.TableStateChangeKind}
	for _, target := range Targets() {
		provider.Conversions = append(provider.Conversions, engine.Conversion{Target: target, Kinds: models, Service: policyconvert.Service{}})
		provider.Comparisons = append(provider.Comparisons, engine.ObjectComparison{Target: target,
			Kinds: []schemaext.Kind{pgpolicy.PolicyKind}, ChangeKinds: []schemaext.Kind{pgpolicy.PolicyChangeKind},
			Service: policycompare.PolicyService{}})
		provider.FacetComparisons = append(provider.FacetComparisons, engine.FacetComparison{Target: target,
			OwnerKinds: []objectidentity.Kind{objectidentity.KindTable}, Kinds: []schemaext.Kind{pgpolicy.TableStateKind},
			ChangeKinds: []schemaext.Kind{pgpolicy.TableStateChangeKind}, Service: policycompare.TableStateService{}})
		provider.Normalizations = append(provider.Normalizations, engine.Normalization{Target: target,
			Kinds: []schemaext.Kind{pgpolicy.PolicyKind}, Service: policyprobe.Service{}})
		provider.Planning = append(provider.Planning, engine.Planning{Target: target, Kinds: changes, ParentKinds: models,
			OperationKinds: []schemaext.Kind{pgpolicy.PolicyOperationKind, pgpolicy.PolicyCommentOperationKind, pgpolicy.TableStateOperationKind},
			Service:        policyplan.Service{}})
		provider.Reversals = append(provider.Reversals, engine.Reversal{Target: target, Kinds: changes, Service: policyreverse.Service{}})
	}
	return provider
}
