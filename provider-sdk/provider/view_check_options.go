package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// ViewCheckOptionsFeature has a declared view state the level of its check
// option in SQLView.check_option: CASCADED or LOCAL, and CHECK_OPTION_NONE
// for a view without one, with SQLView.with_check true exactly when the view
// has a check option. Every SQLView carries it under the feature: a host's
// view creation and declared common objects, and the views a provider reads
// from SQL. A peer that does not list it sends CHECK_OPTION_NONE, and
// with_check alone says whether a view has a check option, as it does for a
// peer of contract 25.31. It is a two-way feature: the provider lists it, and
// so does each engine whose views carry the level. Contract 25.32.
const ViewCheckOptionsFeature = "schema.view-check-options"

// validateViewCheckOptionsFeature refuses an engine that lists the feature
// without the provider's.
func validateViewCheckOptionsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if slices.Contains(engine.GetFeatures(), ViewCheckOptionsFeature) && !slices.Contains(description.GetFeatures(), ViewCheckOptionsFeature) {
		return fmt.Errorf("%w: view check options require the provider's feature", ErrProtocol)
	}
	return nil
}

// validateViewCheckOptions checks the check option of each declared view
// against the negotiation: a level only under the feature, a level this SDK
// knows, and with_check that agrees with it.
func validateViewCheckOptions(description *protocol.Description, engine *protocol.Engine, features []string, views []*protocol.SQLView) error {
	negotiated := EngineOffers(description, engine.GetId(), ViewCheckOptionsFeature) && slices.Contains(features, ViewCheckOptionsFeature)
	for _, view := range views {
		option := view.GetCheckOption()
		if !negotiated {
			if option != protocol.SQLView_CHECK_OPTION_NONE {
				return commonObjectError("the check option level of view " + view.GetName() + " was not negotiated")
			}
			continue
		}
		switch option {
		case protocol.SQLView_CHECK_OPTION_NONE, protocol.SQLView_CASCADED, protocol.SQLView_LOCAL:
		default:
			return commonObjectError("view " + view.GetName() + " states an unknown check option level")
		}
		if view.GetWithCheck() != (option != protocol.SQLView_CHECK_OPTION_NONE) {
			return commonObjectError("view " + view.GetName() + " states with_check exactly when its check option is CASCADED or LOCAL")
		}
	}
	return nil
}
