package embedspec

import (
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embedcutover"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedprovider"
)

// Resolve turns a parsed document into the values the lifecycle works with.
//
// Every enumerated value is checked against the set this build acts on rather
// than passed through. A specification naming a policy Ptah does not implement
// would otherwise produce a generation identity over a word nothing reads, and
// the first sign would be vectors that do not match what the file describes.
func (d Document) Resolve(path string) (Loaded, error) {
	if d.Version != FormatVersion {
		return Loaded{}, fmt.Errorf(
			"%s declares format version %d and this build reads %d", path, d.Version, FormatVersion)
	}
	if d.Source.Mutable == nil {
		// Asked before anything else, because it is the one field whose
		// absence changes what the whole run is. Everything below is a value
		// that is wrong; this is a question nobody answered.
		return Loaded{}, fmt.Errorf(
			"%s does not say whether the source is mutable; a live table planned as a frozen one "+
				"skips every change made while the backfill runs", path)
	}
	mode, err := embedcatchup.ParseMode(d.Consistency.Mode)
	if err != nil {
		return Loaded{}, fmt.Errorf("%s: %w", path, err)
	}
	if _, err := embedprovider.ParseCredentialRef(d.Model.Credential); err != nil {
		return Loaded{}, fmt.Errorf("%s: %w", path, err)
	}
	if err := checkEndpointCarriesNoCredential(d.Model.Endpoint, path); err != nil {
		return Loaded{}, err
	}
	maxPlanAge, err := parseDuration(d.Policy.MaxPlanAge, path)
	if err != nil {
		return Loaded{}, err
	}
	if err := d.checkApprovalPolicy(path); err != nil {
		return Loaded{}, err
	}

	spec, err := d.spec(path)
	if err != nil {
		return Loaded{}, err
	}
	return Loaded{
		Spec: spec,
		Mode: mode,
		Source: embedcatchup.SourceState{
			Mutable: *d.Source.Mutable, Paused: d.Consistency.Paused,
		},
		Policy: embedcutover.Policy{
			RequireExactApproval:   d.Policy.RequireExactApproval,
			RequireSignedApproval:  d.Policy.RequireSignedApproval,
			RequireConsistencyMode: d.Policy.RequireConsistencyMode,
			AllowAcceptedFindings:  d.Policy.AllowAcceptedFindings,
			MaxPlanAge:             maxPlanAge,
		},
		Credential: d.Model.Credential,
		Endpoint:   d.Model.Endpoint,
	}, nil
}

// spec builds the transformation the identity is taken over.
func (d Document) spec(path string) (embedgen.Spec, error) {
	versionStrategy, err := resolveVersionStrategy(d.Source.VersionStrategy, path)
	if err != nil {
		return embedgen.Spec{}, err
	}
	nullPolicy, err := resolveNullPolicy(d.Preprocessing.NullPolicy, path)
	if err != nil {
		return embedgen.Spec{}, err
	}
	emptyPolicy, err := resolveEmptyPolicy(d.Preprocessing.EmptyPolicy, path)
	if err != nil {
		return embedgen.Spec{}, err
	}
	unicodeForm, err := resolveUnicodeForm(d.Preprocessing.UnicodeNormalization, path)
	if err != nil {
		return embedgen.Spec{}, err
	}
	truncate, err := resolveTruncate(d.Preprocessing.Truncate, path)
	if err != nil {
		return embedgen.Spec{}, err
	}
	endpointClass, err := resolveEndpointClass(d.Model.EndpointClass, path)
	if err != nil {
		return embedgen.Spec{}, err
	}
	normalization, err := resolveNormalization(d.Model.Normalization, path)
	if err != nil {
		return embedgen.Spec{}, err
	}
	metric, err := resolveMetric(d.Target.Metric, path)
	if err != nil {
		return embedgen.Spec{}, err
	}

	return embedgen.Spec{
		Name:        d.Name,
		Description: d.Description,
		Source: embedgen.Source{
			Schema: d.Source.Schema, Table: d.Source.Table, Filter: d.Source.Filter,
			KeyFields: d.Source.KeyFields, InputFields: d.Source.InputFields,
			VersionStrategy: versionStrategy, VersionField: d.Source.VersionField,
		},
		Preprocessing: embedgen.Preprocessing{
			Separator: d.Preprocessing.Separator, Prefix: d.Preprocessing.Prefix,
			NullPolicy: nullPolicy, EmptyPolicy: emptyPolicy,
			UnicodeNormalization: unicodeForm,
			CollapseWhitespace:   d.Preprocessing.CollapseWhitespace,
			MaxInputBytes:        d.Preprocessing.MaxInputBytes, Truncate: truncate,
		},
		Model: embedgen.Model{
			Provider: d.Model.Provider, EndpointClass: endpointClass,
			Identifier: d.Model.Identifier, Revision: d.Model.Revision,
			RequestedDimension: d.Model.RequestedDimension,
			ReportedDimension:  d.Model.ReportedDimension,
			Normalization:      normalization, Pooling: d.Model.Pooling,
		},
		Target: embedgen.Target{
			Schema: d.Target.Schema, Table: d.Target.Table, Column: d.Target.Column,
			Representation: d.Target.Representation, Metric: metric,
			IndexMethod: d.Target.IndexMethod, IndexOptions: d.indexOptions(),
		},
	}, nil
}

// indexOptions reads the index options, which are recall tuning and are
// deliberately outside the generation identity.
func (d Document) indexOptions() map[string]string {
	if len(d.Target.IndexOptions) == 0 {
		return nil
	}
	return maps.Clone(d.Target.IndexOptions)
}

// parseDuration reads an optional duration.
func parseDuration(raw, path string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: max_plan_age %q is not a duration: %w", path, raw, err)
	}
	return value, nil
}

// refuse renders the error an unknown enumerated value gets.
func refuse[T ~string](path, field, raw string, known []T) error {
	names := make([]string, 0, len(known))
	for _, value := range known {
		names = append(names, string(value))
	}
	return fmt.Errorf("%s: %s %q is not one this build acts on; it has %s",
		path, field, raw, strings.Join(names, ", "))
}

// resolveEnum maps a raw value onto one of a known set.
func resolveEnum[T ~string](raw, path, field string, known []T) (T, error) {
	candidate := T(strings.TrimSpace(raw))
	if slices.Contains(known, candidate) {
		return candidate, nil
	}
	return T(""), refuse(path, field, raw, known)
}

// resolveVersionStrategy reads source.version_strategy.
func resolveVersionStrategy(raw, path string) (embedgen.VersionStrategy, error) {
	return resolveEnum(raw, path, "source.version_strategy", embedgen.VersionStrategies())
}

// resolveNullPolicy reads preprocessing.null_policy.
func resolveNullPolicy(raw, path string) (embedgen.NullPolicy, error) {
	return resolveEnum(raw, path, "preprocessing.null_policy", embedgen.NullPolicies())
}

// resolveEmptyPolicy reads preprocessing.empty_policy.
func resolveEmptyPolicy(raw, path string) (embedgen.EmptyPolicy, error) {
	return resolveEnum(raw, path, "preprocessing.empty_policy", embedgen.EmptyPolicies())
}

// resolveUnicodeForm reads preprocessing.unicode_normalization.
func resolveUnicodeForm(raw, path string) (embedgen.UnicodeForm, error) {
	return resolveEnum(raw, path, "preprocessing.unicode_normalization", embedgen.UnicodeForms())
}

// resolveTruncate reads preprocessing.truncate.
func resolveTruncate(raw, path string) (embedgen.TruncatePolicy, error) {
	return resolveEnum(raw, path, "preprocessing.truncate", embedgen.TruncatePolicies())
}

// resolveEndpointClass reads model.endpoint_class.
func resolveEndpointClass(raw, path string) (embedgen.EndpointClass, error) {
	return resolveEnum(raw, path, "model.endpoint_class", embedgen.EndpointClasses())
}

// resolveNormalization reads model.normalization.
func resolveNormalization(raw, path string) (embedgen.VectorNormalization, error) {
	return resolveEnum(raw, path, "model.normalization", embedgen.VectorNormalizations())
}

// resolveMetric reads target.metric.
func resolveMetric(raw, path string) (embedgen.DistanceMetric, error) {
	return resolveEnum(raw, path, "target.metric", embedgen.DistanceMetrics())
}

// checkApprovalPolicy refuses a signed-approval requirement standing alone.
//
// A signature is over an exact plan, so "signed but not exact" describes
// nothing: every check that reads the signed requirement sits behind the exact
// one, and a specification setting only the first got no approval requirement
// at all -- a policy line that reads as the strictest setting available and
// silently did nothing (stokaro/ptah#2068).
//
// Refused rather than implied. Implying it would make the file mean something
// it does not say, and an operator reading their own specification back would
// see one requirement where two are in force.
func (d Document) checkApprovalPolicy(path string) error {
	if d.Policy.RequireSignedApproval && !d.Policy.RequireExactApproval {
		return fmt.Errorf(
			"%s: policy.require_signed_approval needs policy.require_exact_approval, "+
				"because a signature is given over one exact plan; set both or neither", path)
	}
	return nil
}

// checkEndpointCarriesNoCredential refuses a model.endpoint with userinfo in it.
//
// The rule `model.credential` enforces is that a key must not appear in project
// configuration at all, and `model.endpoint` is project configuration. Go's
// http client turns `http://user:secret@host/v1` into `Authorization: Basic …`
// on every provider request, so a URL written that way is a credential that
// took a different field to get to the same wire -- and the reference form the
// credential field insists on exists precisely so that a value never enters the
// document.
//
// The refusal belongs here rather than at the request, because a specification
// is copied faster than it is executed. Measured on the shipped binary before
// this check (stokaro/ptah#2644): `probe` reported "no credential was sent" on
// the request that carried it, `plan` printed the whole URL under "What leaves
// the database", `backfill` sent the corpus under the same header, and
// `plan --publish-evidence` wrote the document into the release artifact's
// specification.yaml layer -- so promotion carried the token into the registry
// and into every environment that ran the release. One refusal at the document
// closes all four, and it closes them for a published artifact too, because
// ParsePublished resolves through here as well.
//
// The message names the field and the host and never the userinfo. An error
// that quoted the URL back would print the credential to the terminal and into
// whatever collects the log, which is the exposure rather than a report of it.
func checkEndpointCarriesNoCredential(raw, path string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		// Fail closed, and say why an unparseable value is refused HERE: this
		// check cannot establish that an endpoint it cannot read carries no
		// credential, and an endpoint no URL parser accepts has no request to
		// reach anyway.
		return fmt.Errorf(
			"%s: model.endpoint is not a URL, so it cannot be checked for an embedded credential", path)
	}
	if parsed.User == nil {
		return nil
	}
	return fmt.Errorf(
		"%s: model.endpoint carries a credential in its userinfo, before the %q host; "+
			"a key must not appear in project configuration, so put it in model.credential "+
			"as env:NAME or file:/path",
		path, parsed.Host)
}
