package generator_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/safety"
)

func TestMigrationPlanWriteFiles_PublishesMultiplePairsAndReports(t *testing.T) {
	c := qt.New(t)
	outputDir := c.TempDir()
	firstAssessment := safety.StatementAssessment{
		Index:     1,
		NodeType:  "create_table",
		Subject:   "users",
		Statement: "CREATE TABLE users (id INTEGER);",
		Severity:  safety.Safe,
		Reason:    "creates a new table",
	}
	secondAssessment := safety.StatementAssessment{
		Index:     1,
		NodeType:  "create_index",
		Subject:   "users",
		Statement: "CREATE INDEX CONCURRENTLY users_name_idx ON users (name);",
		Severity:  safety.Warning,
		Reason:    "builds an index on an existing table",
	}
	specs := []generator.MigrationPlanSpecForTest{
		{
			Version:     1700000000,
			Name:        "transactional",
			UpSQL:       "CREATE TABLE users (id INTEGER);\n",
			DownSQL:     "DROP TABLE users;\n",
			Assessments: []safety.StatementAssessment{firstAssessment},
		},
		{
			Version:       1700000001,
			Name:          "concurrent_indexes",
			UpSQL:         "-- +ptah no_transaction\nCREATE INDEX CONCURRENTLY users_name_idx ON users (name);\n",
			DownSQL:       "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY users_name_idx;\n",
			Assessments:   []safety.StatementAssessment{secondAssessment},
			NoTransaction: true,
		},
	}
	plan, err := generator.NewMigrationPlanForTest(outputDir, "json", specs)
	c.Assert(err, qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	firstUp := filepath.Join(outputDir, "1700000000_transactional.up.sql")
	firstDown := filepath.Join(outputDir, "1700000000_transactional.down.sql")
	firstReport := filepath.Join(outputDir, "1700000000_transactional.safety.json")
	secondUp := filepath.Join(outputDir, "1700000001_concurrent_indexes.up.sql")
	secondDown := filepath.Join(outputDir, "1700000001_concurrent_indexes.down.sql")
	secondReport := filepath.Join(outputDir, "1700000001_concurrent_indexes.safety.json")
	expectedPairs := []generator.MigrationFilePair{
		{
			UpFile:     firstUp,
			DownFile:   firstDown,
			ReportFile: firstReport,
			Version:    1700000000,
		},
		{
			UpFile:        secondUp,
			DownFile:      secondDown,
			ReportFile:    secondReport,
			Version:       1700000001,
			NoTransaction: true,
		},
	}
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.DeepEquals, &generator.MigrationFiles{
		UpFile:     firstUp,
		DownFile:   firstDown,
		ReportFile: firstReport,
		Version:    1700000000,
		Files:      expectedPairs,
	})

	firstUpContents, err := os.ReadFile(firstUp)
	c.Assert(err, qt.IsNil)
	c.Assert(string(firstUpContents), qt.Equals, specs[0].UpSQL)
	firstDownContents, err := os.ReadFile(firstDown)
	c.Assert(err, qt.IsNil)
	c.Assert(string(firstDownContents), qt.Equals, specs[0].DownSQL)
	secondUpContents, err := os.ReadFile(secondUp)
	c.Assert(err, qt.IsNil)
	c.Assert(string(secondUpContents), qt.Equals, specs[1].UpSQL)
	secondDownContents, err := os.ReadFile(secondDown)
	c.Assert(err, qt.IsNil)
	c.Assert(string(secondDownContents), qt.Equals, specs[1].DownSQL)

	firstReportContents, err := os.ReadFile(firstReport)
	c.Assert(err, qt.IsNil)
	var firstSafetyReport safety.Report
	c.Assert(json.Unmarshal(firstReportContents, &firstSafetyReport), qt.IsNil)
	c.Assert(firstSafetyReport.Assessments, qt.DeepEquals, specs[0].Assessments)
	secondReportContents, err := os.ReadFile(secondReport)
	c.Assert(err, qt.IsNil)
	var secondSafetyReport safety.Report
	c.Assert(json.Unmarshal(secondReportContents, &secondSafetyReport), qt.IsNil)
	c.Assert(secondSafetyReport.Assessments, qt.DeepEquals, specs[1].Assessments)

	names, err := migrationArtifactNames(outputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(names, qt.DeepEquals, []string{
		"1700000000_transactional.down.sql",
		"1700000000_transactional.safety.json",
		"1700000000_transactional.up.sql",
		"1700000001_concurrent_indexes.down.sql",
		"1700000001_concurrent_indexes.safety.json",
		"1700000001_concurrent_indexes.up.sql",
	})
}

func TestMigrationPlanWriteFiles_CollisionLeavesNoPartialArtifacts(t *testing.T) {
	c := qt.New(t)
	outputDir := c.TempDir()
	reportName := "1700000000_transactional.safety.json"
	reportPath := filepath.Join(outputDir, reportName)
	originalReport := []byte("existing report\n")
	c.Assert(os.WriteFile(reportPath, originalReport, 0600), qt.IsNil)
	plan, err := generator.NewMigrationPlanForTest(
		outputDir,
		"json",
		[]generator.MigrationPlanSpecForTest{{
			Version: 1700000000,
			Name:    "transactional",
			UpSQL:   "CREATE TABLE users (id INTEGER);\n",
			DownSQL: "DROP TABLE users;\n",
			Assessments: []safety.StatementAssessment{{
				Index:    1,
				Severity: safety.Safe,
				Reason:   "creates a new table",
			}},
		}},
	)
	c.Assert(err, qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.ErrorMatches, `error creating migration files: migration directory changed during publication: .* already exists`)
	c.Assert(files, qt.IsNil)
	names, err := migrationArtifactNames(outputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(names, qt.DeepEquals, []string{reportName})
	reportContents, err := os.ReadFile(reportPath)
	c.Assert(err, qt.IsNil)
	c.Assert(reportContents, qt.DeepEquals, originalReport)
}

func TestMigrationPlanWriteFiles_CreatesMissingOutputParents(t *testing.T) {
	c := qt.New(t)
	outputDir := filepath.Join(c.TempDir(), "nested", "migrations")
	plan, err := generator.NewMigrationPlanForTest(
		outputDir,
		"",
		[]generator.MigrationPlanSpecForTest{{
			Version: 1700000000,
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INTEGER);\n",
			DownSQL: "DROP TABLE users;\n",
		}},
	)
	c.Assert(err, qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	upContents, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upContents), qt.Equals, "CREATE TABLE users (id INTEGER);\n")
	downContents, err := os.ReadFile(files.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downContents), qt.Equals, "DROP TABLE users;\n")
}

func migrationArtifactNames(outputDir string) ([]string, error) {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(entries))
	for index, entry := range entries {
		names[index] = entry.Name()
	}
	return names, nil
}
