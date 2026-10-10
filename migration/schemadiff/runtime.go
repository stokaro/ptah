package schemadiff

import (
	"ptah.run/config"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
)

// TargetRuntime supplies the comparison and validation services used when a
// comparison includes target facts. Pure comparisons consume
// only schemapreparation.Runtime and do not require a schema validator.
type TargetRuntime interface {
	schemapreparation.Runtime
	schemavalidation.Service
}

// DatabaseRuntime adds rendering for the live normalization probes used by
// CompareWithDatabase, and the feature owners' own probes. Offline comparison
// does not require either service.
type DatabaseRuntime interface {
	TargetRuntime
	renderer.Service
	schemaext.NormalizationService
}

func selectedComparisonOptions(opts *config.CompareOptions, runtime schemaext.TargetResolver) (*config.CompareOptions, schemaext.TargetSelection, error) {
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}
	if opts.Dialect == "" {
		return opts, schemaext.TargetSelection{}, nil
	}
	selected, err := runtime.ResolveTarget(opts.Dialect)
	if err != nil {
		return nil, schemaext.TargetSelection{}, err
	}
	resolved := *opts
	resolved.Dialect = selected.Name()
	return &resolved, selected, nil
}
