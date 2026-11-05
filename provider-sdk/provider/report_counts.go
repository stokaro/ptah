package provider

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// ReportCountsFeature enables ValueModel.metrics and ReportValues: a model
// declares the object-count metrics its values contribute to, and the owner
// counts captured values offline, as a built-in feature owner's reporting
// service does (ADR 0067). The host cannot count a value itself, because only
// the owner reads its payload. The provider and each engine whose models
// declare metrics list it, and the host lists it in its host features.
// Contract 25.24.
const ReportCountsFeature = "models.report-counts"

// ValueReporter counts captured values offline.
type ValueReporter interface {
	// ReportValues returns one report per value of the request, in order and
	// under its ID: one count for each metric the value's model declares, in
	// the declared order. It reads no database. A value it cannot count fails
	// the whole call.
	ReportValues(context.Context, *protocol.ReportValuesRequest) (*protocol.ValueReports, error)
}

// maxModelMetrics bounds the metrics one model declares, and maxMetricHelp
// the bytes of one help text.
const (
	maxModelMetrics = 64
	maxMetricHelp   = 256
)

func reportCountsError(reason string) error {
	return fmt.Errorf("%w: report counts: %s", ErrProtocol, reason)
}

// validateModelMetrics checks the metrics an engine's models declare: the
// feature listed by the provider and the engine, each metric on a desired or
// observed model with a display name, a valid name once per model and one help
// line, one help per name among the engine's models of one representation, and
// the same metrics as every other engine that declares the model.
func validateModelMetrics(description *protocol.Description, engine *protocol.Engine) error {
	declared := slices.ContainsFunc(engine.GetValueModels(), func(model *protocol.ValueModel) bool { return len(model.GetMetrics()) != 0 })
	listed := slices.Contains(engine.GetFeatures(), ReportCountsFeature)
	switch {
	case !listed && declared:
		return reportCountsError("a model declares metrics without the feature")
	case !listed:
		return nil
	case !slices.Contains(description.GetFeatures(), ReportCountsFeature):
		return reportCountsError("the engine requires the provider's feature")
	case !declared:
		return reportCountsError("the engine lists the feature and no model declares a metric")
	}
	helps := make(map[protocol.ValueInput_Representation]map[string]string)
	for _, model := range engine.GetValueModels() {
		if len(model.GetMetrics()) == 0 {
			continue
		}
		if err := validateMetricDeclaration(model); err != nil {
			return err
		}
		if helps[model.GetRepresentation()] == nil {
			helps[model.GetRepresentation()] = make(map[string]string)
		}
		for _, metric := range model.GetMetrics() {
			if help, found := helps[model.GetRepresentation()][metric.GetName()]; found && help != metric.GetHelp() {
				return reportCountsError("two models give one metric two help texts")
			}
			helps[model.GetRepresentation()][metric.GetName()] = metric.GetHelp()
		}
		for _, other := range description.GetEngines() {
			shared := findValueModel(description, other.GetId(), model.GetKind(), model.GetRepresentation())
			if other.GetId() != engine.GetId() && shared != nil && !sameMetrics(shared.GetMetrics(), model.GetMetrics()) {
				return reportCountsError("engines that share a model declare other metrics for it")
			}
		}
	}
	return nil
}

func validateMetricDeclaration(model *protocol.ValueModel) error {
	if model.GetRepresentation() != protocol.ValueInput_DESIRED && model.GetRepresentation() != protocol.ValueInput_OBSERVED {
		return reportCountsError("only a desired or observed model declares metrics")
	}
	if model.GetDisplayName() == "" {
		return reportCountsError("a model that declares metrics needs a display name")
	}
	if len(model.GetMetrics()) > maxModelMetrics {
		return reportCountsError("a model declares too many metrics")
	}
	seen := make(map[string]bool, len(model.GetMetrics()))
	for _, metric := range model.GetMetrics() {
		if !metricName(metric.GetName()) || seen[metric.GetName()] {
			return reportCountsError("a metric name is invalid or repeated")
		}
		seen[metric.GetName()] = true
		if !metricHelp(metric.GetHelp()) {
			return reportCountsError("a metric's help is not one line")
		}
	}
	return nil
}

// metricName accepts lowercase ASCII letters, digits and underscores that
// start with a letter, the suffix an OpenMetrics family name takes.
func metricName(name string) bool {
	for i, r := range name {
		if (r >= 'a' && r <= 'z') || (i > 0 && (r == '_' || (r >= '0' && r <= '9'))) {
			continue
		}
		return false
	}
	return name != "" && len(name) <= maxMetricHelp
}

func metricHelp(help string) bool {
	return help != "" && len(help) <= maxMetricHelp && utf8.ValidString(help) && strings.TrimSpace(help) == help &&
		!strings.ContainsFunc(help, func(r rune) bool { return unicode.IsControl(r) || r == ' ' || r == ' ' })
}

func sameMetrics(left, right []*protocol.ModelMetric) bool {
	return slices.EqualFunc(left, right, func(a, b *protocol.ModelMetric) bool { return proto.Equal(a, b) })
}

// ValidateReportValuesRequest refuses a request the negotiated description
// does not list ReportCountsFeature for, one for an engine that does not list
// it, and one whose values are not valid inputs of one desired or observed
// representation, each of a model that declares metrics.
func ValidateReportValuesRequest(description *protocol.Description, request *protocol.ReportValuesRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), ReportCountsFeature) {
		return reportCountsError("the feature was not negotiated")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if err := validateModelMetrics(description, engine); err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), ReportCountsFeature) || !slices.Contains(request.GetTargetFeatures(), ReportCountsFeature) {
		return reportCountsError("the engine does not report counts")
	}
	if len(request.GetValues()) == 0 {
		return reportCountsError("the request carries no value")
	}
	inputs := &protocol.NormalizeValuesRequest{Engine: request.GetEngine(), TargetFeatures: request.GetTargetFeatures(), Values: request.GetValues()}
	if err := ValidateValueInputs(description, inputs); err != nil {
		return err
	}
	representation := request.GetValues()[0].GetRepresentation()
	if representation != protocol.ValueInput_DESIRED && representation != protocol.ValueInput_OBSERVED {
		return reportCountsError("only desired or observed values are counted")
	}
	for _, input := range request.GetValues() {
		if input.GetRepresentation() != representation {
			return reportCountsError("the values have two representations")
		}
		if len(findValueModel(description, request.GetEngine(), input.GetValue().GetKind(), representation).GetMetrics()) == 0 {
			return reportCountsError("a value's model declares no metric")
		}
	}
	return nil
}

// ValidateValueReports refuses a reply that is incomplete, answers another
// number of values or answers them in another order, or whose counts for a
// value are not exactly its model's metrics in the declared order, each at
// most math.MaxInt64.
func ValidateValueReports(description *protocol.Description, request *protocol.ReportValuesRequest, reply *protocol.ValueReports) error {
	if err := ValidateReportValuesRequest(description, request); err != nil {
		return err
	}
	if reply == nil || !reply.GetComplete() || len(reply.GetValues()) != len(request.GetValues()) {
		return reportCountsError("the reply does not answer every value")
	}
	for i, input := range request.GetValues() {
		report := reply.GetValues()[i]
		if report.GetId() != input.GetId() {
			return reportCountsError("the reply answers the values in another order")
		}
		metrics := findValueModel(description, request.GetEngine(), input.GetValue().GetKind(), input.GetRepresentation()).GetMetrics()
		if len(report.GetCounts()) != len(metrics) {
			return reportCountsError("a report leaves a metric out or counts one twice")
		}
		for j, count := range report.GetCounts() {
			if count.GetName() != metrics[j].GetName() {
				return reportCountsError("a report counts a metric the model does not declare, or in another order")
			}
			if count.GetValue() > math.MaxInt64 {
				return reportCountsError("a count is above the largest signed 64-bit integer")
			}
		}
	}
	return nil
}
