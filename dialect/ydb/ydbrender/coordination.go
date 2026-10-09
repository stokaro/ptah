// Package ydbrender validates and renders YDB-owned operation payloads.
// It uses captured target capabilities and never opens a database connection.
package ydbrender

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// CoordinationHandler renders standalone transitions through the same local
// dispatch contract used by both visitor and selected runtime entry points.
func CoordinationHandler() renderer.ExtensionHandler {
	return renderer.TypedHandler(&ydbast.CoordinationNode{}, ast.StatementExtension, validateCoordination, renderCoordination)
}

func validateCoordination(ctx renderer.ExtensionContext, value *ydbast.CoordinationNode) error {
	if ctx.Target != "ydb" || !ctx.Capabilities.Has(capability.CoordinationNodes) {
		return &ptaherr.CapabilityError{Dialect: ctx.Target, Feature: string(capability.CoordinationNodes), Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("coordination nodes require target capability %s on YDB", capability.CoordinationNodes)}
	}
	if err := value.Validate(); err != nil {
		return &ptaherr.RenderError{Dialect: ctx.Target, Err: ptaherr.ErrInvalidSchemaDiff, Message: err.Error()}
	}
	return nil
}

func renderCoordination(_ renderer.ExtensionContext, value *ydbast.CoordinationNode) ([]string, error) {
	statement := ydbcoordination.Statement{Path: value.Name}
	if value.Schema != "" {
		statement.Path = value.Schema + "/" + value.Name
	}
	switch {
	case value.Change.Before == nil:
		statement.Verb, statement.Spec = ydbcoordination.Create, value.Change.After.Spec
	case value.Change.After == nil:
		statement.Verb = ydbcoordination.Drop
	default:
		statement.Verb = ydbcoordination.Alter
		statement.Spec = ydbcoordination.Changes(value.Change.After.Spec, value.Change.Before.Spec)
	}
	text, err := statement.Text()
	if err != nil {
		return nil, err
	}
	return []string{text + ";"}, nil
}
