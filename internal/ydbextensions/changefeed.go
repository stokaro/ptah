// Package ydbextensions supplies local validation and rendering handlers for
// YDB-owned AST payloads. The common renderer dispatches by kind and role.
package ydbextensions

import (
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbchangefeed"
)

// Handlers returns fresh registration descriptors for YDB extension payloads.
func Handlers() []renderer.ExtensionHandler {
	return []renderer.ExtensionHandler{
		ydbrender.CoordinationHandler(),
		ydbrender.TopicHandler(),
		ydbrender.TopicConsumerHandler(),
		ydbrender.ExternalDataSourceHandler(),
		ydbrender.ExternalTableHandler(),
		ydbrender.AsyncReplicationHandler(),
		ydbrender.TransferHandler(),
		ydbrender.StreamingHandler(),
		ydbrender.ResourcePoolHandler(),
		ydbrender.ResourcePoolClassifierHandler(),
		ydbrender.DefaultPoolSettingsHandler(),
		ydbrender.SecretHandler(),
		ydbrender.TTLHandler(),
		ydbrender.ColumnFamiliesHandler(),
		ydbrender.TablePartitioningHandler(),
		ydbrender.ColumnStoreTTLHandler(),
		ydbrender.DropVectorIndexHandler(),
		ydbrender.AddVectorIndexHandler(),
		renderer.TypedHandler(&ydbast.AddChangefeed{}, ast.AlterExtension, validateAdd, renderAdd),
		renderer.TypedHandler(&ydbast.DropChangefeed{}, ast.AlterExtension, validateDrop, renderDrop),
		renderer.TypedHandler(&ydbast.AlterChangefeedTopic{}, ast.AlterExtension, validateTopic, renderTopic),
	}
}

// Registry constructs the validated local dispatch table. There is no global
// registration state; both rendering entry points use these same descriptors.
func Registry() (renderer.Extensions, error) { return renderer.NewExtensions(Handlers()...) }

func validateAdd(ctx renderer.ExtensionContext, payload *ydbast.AddChangefeed) error {
	return checkFeed(ctx, payload.Changefeed)
}

func validateDrop(ctx renderer.ExtensionContext, op *ydbast.DropChangefeed) error {
	if ctx.Capabilities.Has(capability.Changefeeds) {
		if strings.TrimSpace(op.Name) == "" || strings.ContainsRune(op.Name, '/') {
			return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff,
				Message: "a changefeed needs a name without a slash"}
		}
		return nil
	}
	subject := fmt.Sprintf("dropping changefeed %q of %s", op.Name, tableref.Phrase(table(ctx)))
	return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(capability.Changefeeds), Err: ptaherr.ErrUnsupportedFeature,
		Message: refusalMessage(ctx.Target, subject, capability.Changefeeds, "")}
}

func validateTopic(ctx renderer.ExtensionContext, op *ydbast.AlterChangefeedTopic) error {
	if op.Previous.Name != op.Changefeed.Name || ydbchangefeed.Recreated(op.Changefeed, op.Previous) {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(op.Kind()), Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("changefeed %q of %s: YDB changes no option of a changefeed in place (`MODE alter is not supported`), so the change drops the changefeed and adds it again", op.Changefeed.Name, tableref.Phrase(table(ctx)))}
	}
	// Topic operations preserve enabled state. Only ADD CHANGEFEED would need
	// a statement to reconstruct a disabled stream.
	spec := op.Changefeed.Clone()
	spec.Disabled = false
	return checkFeed(ctx, spec)
}

func checkCapability(ctx renderer.ExtensionContext) error {
	if ctx.Capabilities.Has(capability.Changefeeds) {
		return nil
	}
	return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(capability.Changefeeds), Err: ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf("changing the changefeeds of %s, which requires target capability changefeeds, unavailable on this %s target", tableref.Phrase(table(ctx)), ctx.Target)}
}

func checkFeed(ctx renderer.ExtensionContext, feed ydbschema.ChangefeedSpec) error {
	if err := checkCapability(ctx); err != nil {
		return err
	}
	if refusal := ydbchangefeed.Check(table(ctx), feed, ctx.Capabilities); refusal != nil {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(refusal.Key), Err: ptaherr.ErrUnsupportedFeature,
			Message: refusalMessage(ctx.Target, refusal.Subject, refusal.Key, refusal.Reason)}
	}
	return nil
}

func table(ctx renderer.ExtensionContext) string {
	if ctx.Parent == nil {
		return ""
	}
	return ctx.Parent.Name
}

func renderAdd(ctx renderer.ExtensionContext, payload *ydbast.AddChangefeed) ([]string, error) {
	return ydbchangefeed.AddStatements(table(ctx), payload.Changefeed), nil
}

func renderDrop(ctx renderer.ExtensionContext, payload *ydbast.DropChangefeed) ([]string, error) {
	return []string{ydbchangefeed.DropStatement(table(ctx), payload.Name)}, nil
}

func renderTopic(ctx renderer.ExtensionContext, op *ydbast.AlterChangefeedTopic) ([]string, error) {
	statements, _ := ydbchangefeed.TopicStatements(table(ctx), op.Changefeed, op.Previous)
	return statements, nil
}

func refusalMessage(target, subject string, key capability.Capability, reason string) string {
	if key != "" {
		return fmt.Sprintf("%s, which requires target capability %s, unavailable on this %s target", subject, key, target)
	}
	return subject + ": " + reason
}
