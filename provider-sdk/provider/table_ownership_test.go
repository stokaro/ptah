package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// ownershipExchange is planningExchange with its service owning the engine's
// tables under negotiated table ownership: after the receipt of its parent
// kind, the reply answers one of kind "table" for the table it alters.
func ownershipExchange() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, result := planningExchange()
	d.Features = append(d.Features, provider.TableOwnershipFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.TableOwnershipFeature)
	d.Engines[0].Planning[0].OwnsTables = true
	request.TargetFeatures = append(request.TargetFeatures, provider.TableOwnershipFeature)
	table := proto.CloneOf(result.Parents[0])
	table.Receipt.Kind, table.Receipt.Strategy = provider.TableReceiptKind, "Alter the table in place"
	result.Parents = append(result.Parents, table)
	return d, request, result
}

// lifecycleOnlyExchange is ownershipExchange with a service that owns the
// tables and plans nothing else: no change, parent or context kind, so the
// request carries no parent kind and the reply one table receipt.
func lifecycleOnlyExchange() (*protocol.Description, *protocol.PlanFeaturesRequest, *protocol.PlannedFeatures) {
	d, request, result := ownershipExchange()
	service := d.Engines[0].Planning[0]
	service.ChangeKinds, service.ParentKinds = nil, nil
	request.ParentKinds, request.Changes = nil, nil
	result.Changes, result.Parents = nil, result.Parents[1:]
	return d, request, result
}

func TestTableOwnership_HappyPath(t *testing.T) {
	t.Run("an owner with a parent kind", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := ownershipExchange()

		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	})
	t.Run("an owner that plans nothing but the tables", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := lifecycleOnlyExchange()

		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.IsNil)
	})
	t.Run("an owner that refuses a table by name", func(t *testing.T) {
		c := qt.New(t)
		d, request, _ := lifecycleOnlyExchange()
		refusal := &protocol.PlannedFeatures{Complete: true, Diagnostics: []*protocol.PlanningDiagnostic{{Code: protocol.PlanningDiagnostic_UNSUPPORTED_FEATURE,
			Kind: provider.TableReceiptKind, Message: "Cannot alter the table", Input: &protocol.PlanningDiagnostic_Parent{Parent: 0}}}}

		c.Assert(provider.ValidatePlannedFeatures(d, request, refusal), qt.IsNil)
	})
}

// A reply accounts for each table with one table receipt after the receipts
// of the parent kinds, in that order.
func TestTableOwnershipReceipts_FailurePath(t *testing.T) {
	t.Run("no table receipt", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := ownershipExchange()
		result.Parents = result.Parents[:1]

		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorMatches, `.*missing planning parent receipt`)
	})
	t.Run("the table receipt before the parent kind's", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := ownershipExchange()
		result.Parents[0], result.Parents[1] = result.Parents[1], result.Parents[0]

		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorMatches, `.*planning receipt changed its input or omitted its strategy`)
	})
	t.Run("a table receipt from a service that does not own tables", func(t *testing.T) {
		c := qt.New(t)
		d, request, result := ownershipExchange()
		d.Engines[0].Planning[0].OwnsTables = false

		c.Assert(provider.ValidatePlannedFeatures(d, request, result), qt.ErrorMatches, `.*extra planning parent receipts`)
	})
}

// Ownership needs the engine's and the provider's feature, one owner on an
// engine, and whole tables; a service that owns nothing and plans nothing is
// still refused.
func TestTableOwnershipDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		// removed is an engine feature the row takes away, and added the
		// features it lists on the provider and the engine.
		removed string
		added   []string
		tables  protocol.PlanningTables
		owners  int
		want    string
	}{
		{"an owner without the engine's feature", provider.TableOwnershipFeature, nil, protocol.PlanningTables_PLANNING_TABLES_CAPTURED, 1,
			`.*planning service retention owns tables without the engine's planning\.table-ownership`},
		{"two owners on one engine", "", nil, protocol.PlanningTables_PLANNING_TABLES_CAPTURED, 2,
			`.*engine fixture has more than one planning service that owns its tables`},
		{"an owner that reads outlines", "", []string{provider.TableOutlinesFeature}, protocol.PlanningTables_PLANNING_TABLES_OUTLINED, 1,
			`.*planning service retention owns tables and reads outlines, which leave out what it plans`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, _, _ := lifecycleOnlyExchange()
			engine := d.Engines[0]
			engine.Features = append(withoutFeature(engine.Features, test.removed), test.added...)
			d.Features = append(d.Features, test.added...)
			engine.Planning[0].Tables = test.tables
			for range test.owners - 1 {
				second := proto.CloneOf(engine.Planning[0])
				second.Id = "second"
				engine.Planning = append(engine.Planning, second)
			}

			err := provider.ValidateDescription(d, nil)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		})
	}
	t.Run("an engine feature without the provider's", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := lifecycleOnlyExchange()
		d.Features = slices.DeleteFunc(d.Features, func(feature string) bool { return feature == provider.TableOwnershipFeature })

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*table ownership requires the provider's feature`)
	})
	t.Run("a service that neither owns tables nor plans a kind", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := lifecycleOnlyExchange()
		d.Engines[0].Planning[0].OwnsTables = false

		c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*missing or duplicate planning service`)
	})
}

// A host that does not list the feature sees no service own tables: the
// negotiated description clears the flag, so a service that plans nothing
// else is refused, and one with a parent kind answers no table receipt.
func TestTableOwnershipNegotiation_HappyPath(t *testing.T) {
	c := qt.New(t)
	unlisted := slices.DeleteFunc(provider.TwoWayFeatures(), func(feature string) bool { return feature == provider.TableOwnershipFeature })
	d, request, result := ownershipExchange()
	// Such a host names the target's features without the one it does not list.
	request.TargetFeatures = slices.DeleteFunc(request.TargetFeatures, func(feature string) bool { return feature == provider.TableOwnershipFeature })

	negotiated := provider.NegotiatedDescription(d, unlisted)

	c.Assert(negotiated.Engines[0].Planning[0].GetOwnsTables(), qt.IsFalse)
	c.Assert(negotiated.Engines[0].Features, qt.Not(qt.Contains), provider.TableOwnershipFeature)
	c.Assert(provider.ValidatePlannedFeatures(negotiated, request, result), qt.ErrorMatches, `.*extra planning parent receipts`)
	result.Parents = result.Parents[:1]
	c.Assert(provider.ValidatePlannedFeatures(negotiated, request, result), qt.IsNil)
	only, _, _ := lifecycleOnlyExchange()
	c.Assert(provider.ValidateDescription(provider.NegotiatedDescription(only, unlisted), nil), qt.ErrorMatches, `.*missing or duplicate planning service`)
}

// An owner without a parent kind is the planning service that owns the
// engine's tables for the features that require one, constraint and primary
// key changes, and a host that does not list planning.object-context keeps
// it, since it plans the tables rather than an object context.
func TestTableOwnerWithoutParentKinds_HappyPath(t *testing.T) {
	t.Run("constraint and primary key changes", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := lifecycleOnlyExchange()
		features := []string{provider.ObservedColumnsFeature, provider.ObservedConstraintsFeature, provider.ConstraintChangesFeature,
			provider.ObservedPrimaryKeysFeature, provider.PrimaryKeyChangesFeature}
		d.Features = append(d.Features, features...)
		d.Engines[0].Features = append(d.Engines[0].Features, features...)

		c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
	})
	t.Run("a host that does not list planning.object-context", func(t *testing.T) {
		c := qt.New(t)
		d, _, _ := lifecycleOnlyExchange()

		negotiated := provider.NegotiatedDescription(d, withoutFeature(provider.TwoWayFeatures(), provider.PlanningObjectContextFeature))

		c.Assert(negotiated.Engines[0].Planning, qt.HasLen, 1)
		c.Assert(negotiated.Engines[0].Planning[0].GetOwnsTables(), qt.IsTrue)
		c.Assert(provider.ValidateDescription(negotiated, nil), qt.IsNil)
	})
}
