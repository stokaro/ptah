package provider

import (
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// RenderOmissionsFeature enables RenderResult.omissions: a complete Render
// reports each declared property or object its units leave out, such as a
// type modifier the engine's type does not keep. A host that does not list
// the feature receives no omissions, and a render that leaves something out
// says nothing about it, as it does for a host of contract 25.32. It is a
// two-way feature that requires rendering: the provider lists it, and so does
// each engine whose render reports omissions. Contract 25.33.
const RenderOmissionsFeature = "render.omissions"

// validateRenderOmissionsFeature refuses an engine that lists the feature
// without the provider's, or on a provider that does not render.
func validateRenderOmissionsFeature(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), RenderOmissionsFeature) {
		return nil
	}
	if !slices.Contains(description.GetFeatures(), RenderOmissionsFeature) || !slices.Contains(description.GetFeatures(), RenderFeature) {
		return fmt.Errorf("%w: render omissions require the provider's feature and rendering", ErrProtocol)
	}
	return nil
}

// validateRenderOmissions checks the omissions of a Render reply: none unless
// the engine and the request's target list the feature, none beside a
// refusal or in an incomplete result, and each one well formed and reported
// once.
func validateRenderOmissions(description *protocol.Description, request *protocol.RenderRequest, result *protocol.RenderResult) error {
	omissions := result.GetOmissions()
	if len(omissions) == 0 {
		return nil
	}
	if !EngineOffers(description, request.GetEngine(), RenderOmissionsFeature) || !slices.Contains(request.GetTargetFeatures(), RenderOmissionsFeature) {
		return fmt.Errorf("%w: %s was not negotiated", ErrProtocol, RenderOmissionsFeature)
	}
	if !result.GetComplete() || result.GetRefusal() != nil {
		return fmt.Errorf("%w: only a complete render reports omissions", ErrProtocol)
	}
	for i, omission := range omissions {
		if err := validateRenderOmission(omission); err != nil {
			return err
		}
		if slices.ContainsFunc(omissions[:i], func(earlier *protocol.RenderOmission) bool { return proto.Equal(earlier, omission) }) {
			return fmt.Errorf("%w: a render reports one omission twice", ErrProtocol)
		}
	}
	return nil
}

// validateRenderOmission refuses an omission without a reason token, a kind
// or a name, and any field that is not one line of text.
func validateRenderOmission(omission *protocol.RenderOmission) error {
	if !omissionReason(omission.GetReason()) {
		return fmt.Errorf("%w: an omission's reason is a lower-case token", ErrProtocol)
	}
	if !diagnosticLine(omission.GetKind()) || !diagnosticLine(omission.GetName()) {
		return fmt.Errorf("%w: an omission names the kind and the name of its object", ErrProtocol)
	}
	for _, text := range []string{omission.GetProperty(), omission.GetDetail(), omission.GetRemedy()} {
		if text != "" && !diagnosticLine(text) {
			return fmt.Errorf("%w: an omission's property, detail and remedy are each one line of text", ErrProtocol)
		}
	}
	return nil
}

// omissionReason accepts a lower-case ASCII token: a letter, then letters,
// digits and underscores, such as "unsupported".
func omissionReason(reason string) bool {
	if reason == "" || len(reason) > maxDiagnosticBytes || reason[0] < 'a' || reason[0] > 'z' {
		return false
	}
	for _, r := range reason {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}
