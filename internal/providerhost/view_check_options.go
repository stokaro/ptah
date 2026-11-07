package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// declaredCheckOption is the level of the check option a declared view
// carries to an engine that lists provider.ViewCheckOptionsFeature: CASCADED
// for a view with a check option, since the host's view model holds no level
// and WITH CHECK OPTION means CASCADED (stokaro/ptah#5647), and
// CHECK_OPTION_NONE for one without. An engine that does not list the feature
// receives CHECK_OPTION_NONE, and with_check alone.
func (t *target) declaredCheckOption(view schemamodel.View) protocol.SQLView_CheckOption {
	if !view.WithCheck || !t.negotiated(provider.ViewCheckOptionsFeature) {
		return protocol.SQLView_CHECK_OPTION_NONE
	}
	return protocol.SQLView_CASCADED
}

// createdCheckOption is the level of the check option a view creation
// carries. The node states LOCAL apart from CASCADED, so an engine that lists
// the feature receives the level the node asks for. One that does not list it
// cannot be asked for LOCAL, so LOCAL is refused by name rather than sent as
// the stricter CASCADED check.
func (t *target) createdCheckOption(view *ast.CreateViewNode) (protocol.SQLView_CheckOption, error) {
	negotiated := t.negotiated(provider.ViewCheckOptionsFeature)
	switch {
	case !view.WithCheck:
		return protocol.SQLView_CHECK_OPTION_NONE, nil
	case view.LocalCheck && !negotiated:
		return protocol.SQLView_CHECK_OPTION_NONE, unsupported(fmt.Sprintf("WITH LOCAL CHECK OPTION of view %q", view.Name))
	case !negotiated:
		return protocol.SQLView_CHECK_OPTION_NONE, nil
	case view.LocalCheck:
		return protocol.SQLView_LOCAL, nil
	default:
		return protocol.SQLView_CASCADED, nil
	}
}

// readCheckOption refuses a view a provider read from SQL with a LOCAL check
// option. The host's view model holds no level (stokaro/ptah#5647), and
// keeping the view as WITH CHECK OPTION would turn it into the stricter
// CASCADED check, as the host's own SQL reader refuses WITH LOCAL CHECK
// OPTION by name.
func readCheckOption(name string, view *protocol.SQLView) error {
	if view.GetCheckOption() == protocol.SQLView_LOCAL {
		return fmt.Errorf("%w: view %q of the SQL schema reading has WITH LOCAL CHECK OPTION, and Ptah's view model holds no check option level",
			ptaherr.ErrUnsupportedFeature, name)
	}
	return nil
}
