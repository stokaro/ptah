package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// Reporting registers one owner for model reports in a source representation.
// Metric names are unique within that representation. Reporting ownership does
// not depend on which targets implement the model's migration semantics.
type Reporting struct {
	Representation schemaext.Representation
	Definitions    []schemaext.ReportDefinition
	Service        schemaext.ReportingService
}

type reportingKey struct {
	kind           schemaext.Kind
	representation schemaext.Representation
}

func (r *Runtime) registerReporting(owner string, declaration Reporting) error {
	if (declaration.Representation != schemaext.Desired && declaration.Representation != schemaext.Observed) || len(declaration.Definitions) == 0 || declaration.Service == nil || nilService(declaration.Service) {
		return fmt.Errorf("%w: incomplete reporting registration", ErrInvalidRegistration)
	}
	index := len(r.reportingServices)
	metrics := make(map[string]bool)
	for _, registered := range r.reportingServices {
		if registered.Representation == declaration.Representation {
			for _, definition := range registered.Definitions {
				for _, metric := range definition.Metrics {
					metrics[metric.Name] = true
				}
			}
		}
	}
	for _, definition := range declaration.Definitions {
		if err := r.validateReportDefinition(owner, declaration.Representation, definition, metrics); err != nil {
			return err
		}
		key := reportingKey{representation: declaration.Representation, kind: definition.Kind}
		if _, exists := r.reports[key]; exists {
			return fmt.Errorf("%w: duplicate reporting for %q/%q", ErrInvalidRegistration, declaration.Representation, definition.Kind)
		}
		r.reports[key] = index
	}
	declaration.Definitions = cloneReportDefinitions(declaration.Definitions)
	slices.SortFunc(declaration.Definitions, func(a, b schemaext.ReportDefinition) int { return strings.Compare(string(a.Kind), string(b.Kind)) })
	r.reportingServices = append(r.reportingServices, declaration)
	return nil
}

func (r *Runtime) validateReportDefinition(owner string, representation schemaext.Representation, definition schemaext.ReportDefinition, metrics map[string]bool) error {
	if !definition.Kind.Valid() || !reportText(definition.DisplayName) {
		return fmt.Errorf("%w: invalid reporting definition for %q", ErrInvalidRegistration, definition.Kind)
	}
	found := slices.ContainsFunc(r.codecs.Definitions(), func(model schemaext.CodecIdentity) bool {
		return model.Kind == definition.Kind && model.Representation == representation && model.Owner == owner
	})
	if !found {
		return fmt.Errorf("%w: %q does not own %q/%s", ErrInvalidRegistration, owner, definition.Kind, representation)
	}
	for _, metric := range definition.Metrics {
		if !metricName(metric.Name) || !reportText(metric.Help) || metrics[metric.Name] {
			return fmt.Errorf("%w: invalid or duplicate reporting metric %q", ErrInvalidRegistration, metric.Name)
		}
		metrics[metric.Name] = true
	}
	return nil
}

func reportText(text string) bool {
	return text != "" && strings.TrimSpace(text) == text && !strings.ContainsFunc(text, func(r rune) bool {
		return unicode.IsControl(r) || r == '\u2028' || r == '\u2029'
	})
}

func metricName(name string) bool {
	for i, r := range name {
		if (r >= 'a' && r <= 'z') || (i > 0 && (r == '_' || (r >= '0' && r <= '9'))) {
			continue
		}
		return false
	}
	return name != ""
}

func cloneReportDefinitions(definitions []schemaext.ReportDefinition) []schemaext.ReportDefinition {
	result := slices.Clone(definitions)
	for i := range result {
		result[i].Metrics = slices.Clone(result[i].Metrics)
	}
	return result
}

// ReportFeatures snapshots and validates the whole batch before calling its
// selected services. One service receives all its values in input order.
// Definitions are sorted by kind; value reports retain caller order. Errors or
// cancellation discard all metadata and value reports, including earlier replies.
func (r *Runtime) ReportFeatures(ctx context.Context, request schemaext.ReportingRequest) (schemaext.FeatureReport, error) {
	if err := schemaext.RequireRuntime(ctx, r); err != nil {
		return schemaext.FeatureReport{}, err
	}
	values, err := r.codecs.SnapshotValues(ctx, request.Representation, request.Values)
	if err != nil {
		return schemaext.FeatureReport{}, err
	}
	batches := make([][]int, len(r.reportingServices))
	for i, value := range values {
		service, found := r.reports[reportingKey{representation: request.Representation, kind: value.Kind()}]
		if !found {
			return schemaext.FeatureReport{}, fmt.Errorf("%w: no reporting for %q/%q", ptaherr.ErrUnsupportedFeature, request.Representation, value.Kind())
		}
		batches[service] = append(batches[service], i)
	}
	result := schemaext.FeatureReport{Values: make([]schemaext.ValueReport, len(values))}
	for i, registration := range r.reportingServices {
		if registration.Representation != request.Representation {
			continue
		}
		result.Definitions = append(result.Definitions, cloneReportDefinitions(registration.Definitions)...)
		if len(batches[i]) == 0 {
			continue
		}
		batch := schemaext.ReportingRequest{Target: request.Target, Representation: request.Representation}
		kinds := make([]schemaext.Kind, 0, len(batches[i]))
		for _, position := range batches[i] {
			batch.Values = append(batch.Values, values[position])
			kinds = append(kinds, values[position].Kind())
		}
		reply, err := reportBatch(ctx, registration, batch, kinds)
		if err != nil {
			return schemaext.FeatureReport{}, err
		}
		for j, position := range batches[i] {
			result.Values[position] = reply[j]
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FeatureReport{}, err
	}
	slices.SortFunc(result.Definitions, func(a, b schemaext.ReportDefinition) int { return strings.Compare(string(a.Kind), string(b.Kind)) })
	return result, nil
}

func reportBatch(ctx context.Context, registration Reporting, request schemaext.ReportingRequest, kinds []schemaext.Kind) ([]schemaext.ValueReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reply, err := registration.Service.ReportValues(ctx, request)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(reply) != len(kinds) {
		return nil, fmt.Errorf("%w: reporting changed the value count", schemaext.ErrInvalidValue)
	}
	reply = slices.Clone(reply)
	for i, kind := range kinds {
		if reply[i].Kind != kind {
			return nil, fmt.Errorf("%w: reporting changed the ordered kind", schemaext.ErrInvalidValue)
		}
		definition := slices.IndexFunc(registration.Definitions, func(definition schemaext.ReportDefinition) bool { return definition.Kind == kind })
		counts, err := validatedCounts(reply[i].Counts, registration.Definitions[definition].Metrics)
		if err != nil {
			return nil, err
		}
		reply[i].Counts = counts
	}
	return reply, nil
}

func validatedCounts(counts []schemaext.MetricCount, definitions []schemaext.MetricDefinition) ([]schemaext.MetricCount, error) {
	if len(counts) != len(definitions) {
		return nil, fmt.Errorf("%w: reporting changed the metric count", schemaext.ErrInvalidValue)
	}
	seen := make(map[string]bool, len(counts))
	for _, count := range counts {
		known := slices.ContainsFunc(definitions, func(definition schemaext.MetricDefinition) bool { return definition.Name == count.Name })
		if !known || seen[count.Name] || count.Value < 0 {
			return nil, fmt.Errorf("%w: invalid reported metric %q", schemaext.ErrInvalidValue, count.Name)
		}
		seen[count.Name] = true
	}
	result := slices.Clone(counts)
	slices.SortFunc(result, func(a, b schemaext.MetricCount) int { return strings.Compare(a.Name, b.Name) })
	return result, nil
}
