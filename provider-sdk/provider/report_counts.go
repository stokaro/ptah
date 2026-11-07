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

// ReportCountsFeature enables Engine.reports and ReportValues: an engine
// declares the object-count metrics each value of a model contributes to, and
// the owner counts captured values offline, as a built-in feature owner's
// reporting service does (ADR 0067). The host cannot count a value itself,
// because only the owner reads its payload. The metrics live on the engine
// rather than on the model, so a model's identity is the same whether or not
// a host negotiates them. The provider and each engine that declares reports
// list it, and the host lists it in its host features. Contract 25.21.
const ReportCountsFeature = "models.report-counts"

// OmissionCountsFeature enables ModelReport.omission_label and
// ModelReport.omission_count_metric: how an export names and counts the
// objects a value of the model holds when the export leaves the value out.
// The provider and each engine whose reports set either field list it beside
// ReportCountsFeature, and the host lists it when its exports read them. A
// host that does not list it receives reports without the two fields and
// counts one omitted object per value. Contract 25.36.
const OmissionCountsFeature = "models.omission-counts"

// ValueReporter counts captured values offline.
type ValueReporter interface {
	// ReportValues returns one report per value of the request, in order and
	// under its ID: one count for each metric the engine's report of the
	// value's model declares, in the declared order. It reads no database. A
	// value it cannot count fails the whole call.
	ReportValues(context.Context, *protocol.ReportValuesRequest) (*protocol.ValueReports, error)
}

// maxModelMetrics bounds the metrics one report declares, and maxMetricHelp
// the bytes of one name or help text.
const (
	maxModelMetrics = 64
	maxMetricHelp   = 256
)

func reportCountsError(reason string) error {
	return fmt.Errorf("%w: report counts: %s", ErrProtocol, reason)
}

// modelReport returns the report an engine declares for one of its models,
// or nil.
func modelReport(engine *protocol.Engine, kind string, representation protocol.ValueInput_Representation) *protocol.ModelReport {
	for _, report := range engine.GetReports() {
		if report.GetKind() == kind && report.GetRepresentation() == representation {
			return report
		}
	}
	return nil
}

// validateModelReports checks the reports an engine declares: the feature
// listed by the provider and the engine, each report naming one desired or
// observed model of the engine that has a display name, once, with one to
// maxModelMetrics metrics, each a valid name once and one help line, one help
// per name among the engine's reports of one representation, and the same
// report for a model as every other engine that declares the model.
func validateModelReports(description *protocol.Description, engine *protocol.Engine) error {
	listed := slices.Contains(engine.GetFeatures(), ReportCountsFeature)
	switch {
	case !listed && len(engine.GetReports()) != 0:
		return reportCountsError("the engine declares reports without the feature")
	case !listed:
		return nil
	case !slices.Contains(description.GetFeatures(), ReportCountsFeature):
		return reportCountsError("the engine requires the provider's feature")
	case len(engine.GetReports()) == 0:
		return reportCountsError("the engine lists the feature and reports no model")
	}
	helps := make(map[protocol.ValueInput_Representation]map[string]string)
	for _, report := range engine.GetReports() {
		if modelReport(engine, report.GetKind(), report.GetRepresentation()) != report {
			return reportCountsError("the engine reports a model twice")
		}
		if err := validateModelReport(description, engine, report); err != nil {
			return err
		}
		if helps[report.GetRepresentation()] == nil {
			helps[report.GetRepresentation()] = make(map[string]string)
		}
		for _, metric := range report.GetMetrics() {
			if help, found := helps[report.GetRepresentation()][metric.GetName()]; found && help != metric.GetHelp() {
				return reportCountsError("two reports give one metric two help texts")
			}
			helps[report.GetRepresentation()][metric.GetName()] = metric.GetHelp()
		}
	}
	for _, model := range engine.GetValueModels() {
		for _, other := range description.GetEngines() {
			if other.GetId() == engine.GetId() || findValueModel(description, other.GetId(), model.GetKind(), model.GetRepresentation()) == nil {
				continue
			}
			mine, theirs := modelReport(engine, model.GetKind(), model.GetRepresentation()), modelReport(other, model.GetKind(), model.GetRepresentation())
			if !sameMetrics(mine.GetMetrics(), theirs.GetMetrics()) {
				return reportCountsError("engines that share a model report other metrics for it")
			}
			if mine.GetOmissionLabel() != theirs.GetOmissionLabel() ||
				mine.GetOmissionCountMetric() != theirs.GetOmissionCountMetric() {
				return reportCountsError("engines that share a model report other omission metadata for it")
			}
		}
	}
	return nil
}

// validateOmissionCounts holds an engine that lists OmissionCountsFeature to
// ReportCountsFeature beside it and to the provider's listing of the feature:
// the omission fields live on its reports.
func validateOmissionCounts(description *protocol.Description, engine *protocol.Engine) error {
	if !slices.Contains(engine.GetFeatures(), OmissionCountsFeature) {
		return nil
	}
	if !slices.Contains(engine.GetFeatures(), ReportCountsFeature) || !slices.Contains(description.GetFeatures(), OmissionCountsFeature) {
		return reportCountsError("an engine that lists " + OmissionCountsFeature + " lists " + ReportCountsFeature + ", and so does the provider")
	}
	return nil
}

func validateModelReport(description *protocol.Description, engine *protocol.Engine, report *protocol.ModelReport) error {
	if report.GetRepresentation() != protocol.ValueInput_DESIRED && report.GetRepresentation() != protocol.ValueInput_OBSERVED {
		return reportCountsError("only a desired or observed model is reported")
	}
	model := findValueModel(description, engine.GetId(), report.GetKind(), report.GetRepresentation())
	if model == nil {
		return reportCountsError("a report names no model of the engine")
	}
	if model.GetDisplayName() == "" {
		return reportCountsError("a reported model needs a display name")
	}
	if len(report.GetMetrics()) == 0 || len(report.GetMetrics()) > maxModelMetrics {
		return reportCountsError("a report declares no metric or too many")
	}
	seen := make(map[string]bool, len(report.GetMetrics()))
	for _, metric := range report.GetMetrics() {
		if !metricName(metric.GetName()) || seen[metric.GetName()] {
			return reportCountsError("a metric name is invalid or repeated")
		}
		seen[metric.GetName()] = true
		if !metricHelp(metric.GetHelp()) {
			return reportCountsError("a metric's help is not one line")
		}
	}
	if (report.GetOmissionLabel() != "" || report.GetOmissionCountMetric() != "") &&
		(!slices.Contains(engine.GetFeatures(), OmissionCountsFeature) || !slices.Contains(description.GetFeatures(), OmissionCountsFeature)) {
		return reportCountsError("an omission label or count metric needs " + OmissionCountsFeature)
	}
	if label := report.GetOmissionLabel(); label != "" && !metricHelp(label) {
		return reportCountsError("an omission label is not one line")
	}
	if name := report.GetOmissionCountMetric(); name != "" && !seen[name] {
		return reportCountsError("an omission count metric is not declared")
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
// representation, each of a model the engine reports.
func ValidateReportValuesRequest(description *protocol.Description, request *protocol.ReportValuesRequest) error {
	if request == nil || !slices.Contains(description.GetFeatures(), ReportCountsFeature) {
		return reportCountsError("the feature was not negotiated")
	}
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
		return err
	}
	if err := validateModelReports(description, engine); err != nil {
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
		if modelReport(engine, input.GetValue().GetKind(), representation) == nil {
			return reportCountsError("the engine reports no model of a value")
		}
	}
	return nil
}

// ValidateValueReports refuses a reply that is incomplete, answers another
// number of values or answers them in another order, or whose counts for a
// value are not exactly the metrics of its model's report in the declared
// order, each at most math.MaxInt64.
func ValidateValueReports(description *protocol.Description, request *protocol.ReportValuesRequest, reply *protocol.ValueReports) error {
	if err := ValidateReportValuesRequest(description, request); err != nil {
		return err
	}
	return ValidateValueReportsReply(description, request, reply)
}

// ValidateValueReportsReply is [ValidateValueReports] for a request that
// [ValidateReportValuesRequest] accepted with description and that has not
// changed since: it checks the reply and not the request again. The SDK's
// client and server validate each request once and call it for the reply.
func ValidateValueReportsReply(description *protocol.Description, request *protocol.ReportValuesRequest, reply *protocol.ValueReports) error {
	engine, _, err := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err != nil {
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
		metrics := modelReport(engine, input.GetValue().GetKind(), input.GetRepresentation()).GetMetrics()
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
