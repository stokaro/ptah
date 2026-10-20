package planner

import (
	"ptah.run/core/featureplan"
	"ptah.run/core/planext"
	"ptah.run/core/renderer"
)

// PlanningRuntime supplies common and feature planning from the same selection.
type PlanningRuntime interface {
	featureplan.Runtime
	planext.Resolver
}

// Runtime supplies the selected feature planner, local codecs, and renderer.
// Every generation entry point requires it, including an empty migration. The
// caller retains one provider selection throughout planning and rendering.
type Runtime interface {
	PlanningRuntime
	renderer.Service
}
