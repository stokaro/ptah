package pgrole

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/sqlident"
)

// SettingsService converts, compares, plans, reverses and reports PostgreSQL
// role settings through data contracts. Its zero value is concurrent-safe
// and performs no database access.
type SettingsService struct{}

func settingsContext(ctx context.Context, target string) error {
	if ctx == nil {
		return fmt.Errorf("%w: role settings require a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !SupportsSettings(target) {
		return fmt.Errorf("%w: PostgreSQL role settings on %q", ptaherr.ErrUnsupportedDialect, target)
	}
	return nil
}

// ConvertFeatures projects declarations as observations or captures
// observations as declarations, in order and without changing its inputs. A
// declared value becomes the text the catalog stores ([StoredValue]) and a
// declared reset becomes nothing, which is what the role holds after it. An
// observed value becomes the value in SQL ([SQLValue]). Invalid values,
// unsupported targets and cancellation return no partial result.
func (SettingsService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if err := settingsContext(ctx, request.Target); err != nil {
		return nil, err
	}
	if request.From == request.To || request.From != schemaext.Desired && request.From != schemaext.Observed || request.To != schemaext.Desired && request.To != schemaext.Observed {
		return nil, fmt.Errorf("%w: invalid role settings conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertSettings(request.From, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return completed(ctx, result)
}

func convertSettings(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch s := value.(type) {
	case *DesiredSettings:
		if from == schemaext.Desired {
			if err := ValidateDesiredSettings(s); err != nil {
				return nil, err
			}
			observed := &ObservedSettings{Settings: []Setting{}}
			for _, setting := range s.Settings {
				if !setting.Reset {
					observed.Settings = append(observed.Settings, Setting{Name: setting.Name, Value: StoredValue(setting.Name, setting.Value)})
				}
			}
			return observed, nil
		}
	case *ObservedSettings:
		if from == schemaext.Observed {
			if err := ValidateObservedSettings(s); err != nil {
				return nil, err
			}
			desired := &DesiredSettings{Settings: make([]Setting, 0, len(s.Settings))}
			for _, setting := range s.Settings {
				spelled, _, err := ParseValue(SQLValue(setting.Name, setting.Value))
				if err != nil {
					return nil, err
				}
				desired.Settings = append(desired.Settings, Setting{Name: setting.Name, Value: spelled})
			}
			return desired, nil
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s role settings value %T", schemaext.ErrInvalidValue, from, value)
}

// CompareFacets compares the settings of roles present on both sides. Only a
// setting the declaration names is compared, so a role without the facet, or
// a source that cannot spell it, leaves every setting unmanaged. A complete
// observation without the facet holds no settings; an observation that does
// not describe them leaves a declared setting undecided. A role created or
// dropped is not compared here: CREATE ROLE carries its settings and DROP
// ROLE removes them. Cancellation and invalid input return a zero result.
func (SettingsService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := settingsContext(ctx, request.Target); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{SettingsKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported role settings comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if !HoldsSettings(owner.Subject) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: role settings require a role, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
		change, reason, err := compareRole(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if change != nil {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: SettingsKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change}})
		}
		if reason != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: SettingsKind, Subject: owner.Subject, Reason: reason})
		}
	}
	return completed(ctx, result)
}

func compareRole(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (*SettingsChange, string, error) {
	desired, _, err := schemaext.FacetAs[*DesiredSettings](recordFacets(request.Desired, owner.Subject), SettingsKind)
	if err != nil {
		return nil, "", err
	}
	current, _, err := schemaext.FacetAs[*ObservedSettings](recordFacets(request.Current, owner.Subject), SettingsKind)
	if err != nil {
		return nil, "", err
	}
	if desired != nil {
		if err := ValidateDesiredSettings(desired); err != nil {
			return nil, "", err
		}
	}
	if current != nil {
		if err := ValidateObservedSettings(current); err != nil {
			return nil, "", err
		}
	}
	if desired == nil || len(desired.Settings) == 0 || !owner.Desired || !owner.Current || !request.Includes(SettingsKind, owner.Subject) {
		return nil, "", nil
	}
	if reason := knowledgeLimit(request, owner.Subject); reason != "" {
		return nil, reason, nil
	}
	var held []Setting
	if current != nil {
		held = current.Settings
	} else if !describes(request.Current.Coverage, owner.Subject) {
		return nil, "the current source does not describe role settings", nil
	}
	return Changes(desired.Settings, held), "", nil
}

func recordFacets(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}

func describes(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(SettingsKind, subject).State {
	case schemaext.Complete, schemaext.Absent, schemaext.Defaulted:
		return true
	default:
		return false
	}
}

func knowledgeLimit(request schemaext.FacetComparisonRequest, subject objectidentity.ID) string {
	for _, coverage := range []schemaext.Coverage{request.Desired.Coverage, request.Current.Coverage} {
		knowledge := coverage.Lookup(SettingsKind, subject)
		explicit := !coverage.SelectKinds([]schemaext.Kind{SettingsKind}).IsZero()
		if explicit && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable) && knowledge.Reason != "" {
			return knowledge.Reason
		}
	}
	return ""
}

// PlanFeatures plans each change as ALTER ROLE statements on a role both sides
// hold, after the statements that create or alter roles. It takes no parent
// kinds: a role is not a table. Invalid batches and cancellation return no
// executable prefix, and inputs remain unchanged.
func (SettingsService) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := settingsContext(ctx, request.Target); err != nil {
		return featureplan.Result{}, err
	}
	if len(request.ParentKinds) != 0 {
		return featureplan.Result{}, fmt.Errorf("%w: role settings belong to no table", schemaext.ErrInvalidValue)
	}
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: Owner}
	for index, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return featureplan.Result{}, err
		}
		change, ok := record.Value.(*SettingsChange)
		if !ok || !HoldsSettings(record.Subject) {
			return featureplan.Result{}, fmt.Errorf("%w: role settings planning requires a settings change on a role", schemaext.ErrInvalidValue)
		}
		operation := &AlterSettings{Role: RoleName(record.Subject), Change: *change.Copy()}
		if err := operation.Validate(); err != nil {
			return featureplan.Result{}, err
		}
		settings := record.Subject
		settings.Kind = objectidentity.Kind(SettingsKind)
		step := plangraph.Step[featureplan.Operation]{
			ID:          plangraph.StepID{Owner: Owner, Name: fmt.Sprintf("role-settings/%06d", index)},
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Phase: featureplan.PhaseDependent, Payload: operation},
			Effects:     []plangraph.Effect{{Subject: record.Subject, Action: plangraph.Read}, {Subject: settings, Action: plangraph.Alter}},
			Transaction: plangraph.TransactionAllowed, Impact: operation.Effect(),
		}
		contribution.Steps = append(contribution.Steps, step)
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: record.Subject, Kind: SettingsChangeKind,
			Strategy: "set and reset the declared role settings after the role exists", Steps: []plangraph.StepID{step.ID}})
	}
	if len(contribution.Steps) != 0 {
		result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	}
	return completed(ctx, result)
}

// RoleName is the name of the role subject identifies, without quotes.
func RoleName(subject objectidentity.ID) string {
	return sqlident.UnquoteDoubleQuoted(subject.Name.Source)
}

// ReverseChanges returns, for each change, the change that restores what the
// role held before it. It preserves order and inputs; errors and cancellation
// return no partial batch.
func (SettingsService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := settingsContext(ctx, request.Target); err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*SettingsChange)
		if !ok || !HoldsSettings(record.Subject) {
			return nil, fmt.Errorf("%w: reversal requires a settings change on a role", schemaext.ErrInvalidValue)
		}
		inverse, err := change.Reverse()
		if err != nil {
			return nil, err
		}
		result = append(result, schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: inverse},
			Strategy: "restore the role settings the role held before"})
	}
	return completed(ctx, result)
}

// metricSettings is the metric that counts the settings a role holds or a
// declaration names.
const metricSettings = "postgres_role_settings"

// SettingsReports names the feature and counts the settings values hold.
// Counts describe values, not the completeness of an inspection.
func SettingsReports() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: SettingsKind, DisplayName: "PostgreSQL role settings",
		Metrics: []schemaext.MetricDefinition{{Name: metricSettings, Help: "Role configuration settings a declaration names or a role holds"}}}}
}

// ReportValues reports an ordered batch without inspecting a server. Invalid
// representations, values, nil contexts and cancellation return no partial
// batch.
func (SettingsService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: role settings reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Representation != schemaext.Desired && request.Representation != schemaext.Observed {
		return nil, fmt.Errorf("%w: role settings reporting requires a schema representation", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		count, err := settingsCount(request.Representation, value)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: SettingsKind, Counts: []schemaext.MetricCount{{Name: metricSettings, Value: count}}})
	}
	return completed(ctx, reports)
}

func settingsCount(representation schemaext.Representation, value schemaext.Value) (int, error) {
	switch s := value.(type) {
	case *DesiredSettings:
		if representation == schemaext.Desired {
			if err := ValidateDesiredSettings(s); err != nil {
				return 0, err
			}
			return len(s.Settings), nil
		}
	case *ObservedSettings:
		if representation == schemaext.Observed {
			if err := ValidateObservedSettings(s); err != nil {
				return 0, err
			}
			return len(s.Settings), nil
		}
	}
	return 0, fmt.Errorf("%w: unexpected %s role settings value %T", schemaext.ErrInvalidValue, representation, value)
}

// A cancellation at the reply boundary discards the complete batch too.
func completed[T any](ctx context.Context, result T) (T, error) {
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}
	return result, nil
}
