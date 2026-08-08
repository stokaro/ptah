package generator

// White-box testing required: this file exposes planned specifications and an
// interrupted-publication fixture to external-package publication tests.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"

	"go.5x5.cz/ptah/migration/safety"
)

type pendingPublicationJournalForTest struct {
	Version    int                              `json:"version"`
	CommitMode string                           `json:"commit_mode"`
	Entries    []pendingPublicationEntryForTest `json:"entries"`
}

type pendingPublicationEntryForTest struct {
	Staged string `json:"staged"`
	Final  string `json:"final"`
	Mode   string `json:"mode"`
	Digest string `json:"digest"`
}

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
// allowedOutputRoot is the confinement root the publication must stay inside,
// or empty for the direct-CLI shape.
//
// It binds and holds the migration directory exactly as PlanMigration does,
// because that binding is what the plan is: a test plan built any other way
// would be measuring a shape the product never produces.
func NewMigrationPlanForTest(
	outputDir, allowedOutputRoot, reportFormat string,
	specs []MigrationPlanSpecForTest,
) (*MigrationPlan, error) {
	writer, err := bindPlannedMigrationDir(allowedOutputRoot, outputDir)
	if err != nil {
		return nil, err
	}
	plannedContents, err := captureMigrationDirectoryContents(writer)
	if err != nil {
		return nil, errors.Join(err, writer.Close())
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
		outputDir:       outputDir,
		dir:             writer,
		plannedContents: plannedContents,
		reportFormat:    reportFormat,
		specs:           generatedSpecs,
	}, nil
}

// WritePendingPublicationForTest creates an interrupted, uncommitted batch.
func WritePendingPublicationForTest(outputDir string) ([]string, error) {
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, err
	}
	contents := []byte("CREATE TABLE interrupted_items (id INTEGER);\n")
	finalName := "9999999999_interrupted.up.sql"
	stagedName := ".ptah-migrate-diff-recovery-test.tmp"
	finalPath := filepath.Join(outputDir, finalName)
	stagedPath := filepath.Join(outputDir, stagedName)
	if err := os.WriteFile(finalPath, contents, 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(stagedPath, contents, 0o600); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(contents)
	journal := pendingPublicationJournalForTest{
		Version:    5,
		CommitMode: "journal-marker",
		Entries: []pendingPublicationEntryForTest{{
			Staged: stagedName,
			Final:  finalName,
			Mode:   "exclusive-copy",
			Digest: hex.EncodeToString(digest[:]),
		}},
	}
	journalContents, err := json.Marshal(journal)
	if err != nil {
		return nil, err
	}
	journalPath := filepath.Join(
		filepath.Dir(outputDir),
		"."+filepath.Base(outputDir)+".ptah-migrate-diff.pending",
	)
	if err := os.WriteFile(journalPath, journalContents, 0o600); err != nil {
		return nil, err
	}
	return []string{finalPath, stagedPath, journalPath}, nil
}
