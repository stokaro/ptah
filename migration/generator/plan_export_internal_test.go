package generator

// White-box testing required: this file exposes planned specifications only to
// the external-package transactional publication tests.

import (
	"slices"

	"github.com/stokaro/ptah/migration/safety"
)

// MigrationPlanSpecForTest exposes one planned artifact pair to black-box tests.
type MigrationPlanSpecForTest struct {
	Version       int64
	Name          string
	UpSQL         string
	DownSQL       string
	Assessments   []safety.StatementAssessment
	NoTransaction bool
}

// NewMigrationPlanForTest creates a plan without database-dependent planning.
func NewMigrationPlanForTest(
	outputDir, reportFormat string,
	specs []MigrationPlanSpecForTest,
) (*MigrationPlan, error) {
	outputState, err := captureMigrationDirectoryState(outputDir)
	if err != nil {
		return nil, err
	}
	generatedSpecs := make([]generatedMigrationSpec, len(specs))
	for index, spec := range specs {
		generatedSpecs[index] = generatedMigrationSpec{
			Version:       spec.Version,
			Name:          spec.Name,
			UpSQL:         spec.UpSQL,
			DownSQL:       spec.DownSQL,
			Assessments:   slices.Clone(spec.Assessments),
			NoTransaction: spec.NoTransaction,
		}
	}
	return &MigrationPlan{
		outputDir:    outputDir,
		outputState:  outputState,
		reportFormat: reportFormat,
		specs:        generatedSpecs,
	}, nil
}
