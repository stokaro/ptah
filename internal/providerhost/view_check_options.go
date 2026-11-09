package providerhost

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// declaredCheckOption is the level of the check option a declared view
// carries to the engine: the level the view states, LOCAL or CASCADED, to an
// engine that lists provider.ViewCheckOptionsFeature, and CHECK_OPTION_NONE
// beside with_check alone to one that does not. Such an engine reads
// with_check as WITH CHECK OPTION, the CASCADED level, so a LOCAL view is
// refused by name rather than sent as the stricter check.
func (t *target) declaredCheckOption(view schemamodel.View) (protocol.SQLView_CheckOption, error) {
	return t.checkOption(view.Name, view.WithCheck, view.LocalCheck)
}

// createdCheckOption is the level of the check option a view creation
// carries, decided as [target.declaredCheckOption] decides it for a declared
// view.
func (t *target) createdCheckOption(view *ast.CreateViewNode) (protocol.SQLView_CheckOption, error) {
	return t.checkOption(view.Name, view.WithCheck, view.LocalCheck)
}

// checkOption is the level the engine receives for a view named name; see
// [target.declaredCheckOption].
func (t *target) checkOption(name string, withCheck, local bool) (protocol.SQLView_CheckOption, error) {
	negotiated := t.negotiated(provider.ViewCheckOptionsFeature)
	switch {
	case !withCheck:
		return protocol.SQLView_CHECK_OPTION_NONE, nil
	case local && !negotiated:
		return protocol.SQLView_CHECK_OPTION_NONE, unsupported(fmt.Sprintf("WITH LOCAL CHECK OPTION of view %q", name))
	case !negotiated:
		return protocol.SQLView_CHECK_OPTION_NONE, nil
	case local:
		return protocol.SQLView_LOCAL, nil
	default:
		return protocol.SQLView_CASCADED, nil
	}
}

// localCheckOption reports whether a view a provider sent states the LOCAL
// level. A view from an engine that does not list the feature states none,
// and its with_check alone is the CASCADED level.
func localCheckOption(view *protocol.SQLView) bool {
	return view.GetWithCheck() && view.GetCheckOption() == protocol.SQLView_LOCAL
}
