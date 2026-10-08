package planner

import (
	"ptah.run/core/featureplan"
	"ptah.run/core/renderer"
)

// Runtime supplies the selected feature planner, local codecs, and renderer.
// Every generation entry point requires it, including an empty migration. The
// caller retains one provider selection throughout planning and rendering.
type Runtime interface {
	featureplan.Runtime
	renderer.Service
}
