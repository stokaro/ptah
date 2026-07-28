package goannotationcleanup_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/goannotationcleanup"
)

func TestCleanDirDryRunDiffAndWrite(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	original := `package models

// User is business documentation.
//migrator:schema:table name="users"
type User struct {
	// ID is business documentation.
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:embedded mode="inline"
	Timestamps
}
`
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)
	c.Assert(os.Chmod(path, 0o644), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	results := plan.DiffResults()
	c.Assert(results, qt.HasLen, 1)
	c.Assert(results[0].RemovedLines, qt.Equals, 3)
	c.Assert(results[0].Diff, qt.Contains, `-//migrator:schema:table name="users"`)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, original)

	plan, err = goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	results = plan.Results()
	c.Assert(results, qt.HasLen, 1)
	c.Assert(plan.Apply(), qt.IsNil)
	content, err = os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Contains, "// User is business documentation.")
	c.Assert(string(content), qt.Contains, "// ID is business documentation.")
	c.Assert(string(content), qt.Not(qt.Contains), "migrator:schema")
	c.Assert(string(content), qt.Not(qt.Contains), "migrator:embedded")
	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o644))

	plan, err = goannotationcleanup.PlanDir(dir)
	c.Assert(err, qt.IsNil)
	results = plan.Results()
	c.Assert(results, qt.HasLen, 0)
	c.Assert(plan.Apply(), qt.IsNil)
}

func TestCleanDirPreservesUnrelatedFormattingByteForByte(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	original := "package models\n\n// User is business documentation.\n//migrator:schema:table name=\"users\"\ntype User struct{ID int64}\n"
	expected := "package models\n\n// User is business documentation.\ntype User struct{ID int64}\n"
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	results := plan.Results()
	c.Assert(results, qt.HasLen, 1)
	c.Assert(plan.Apply(), qt.IsNil)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, expected)
}

func TestCleanDirDiffReportsDuplicateRemovedLinesByPosition(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	annotation := "//migrator:schema:field name=\"id\" type=\"SERIAL\"\n"
	original := "package models\n\ntype User struct {\n" +
		annotation +
		"ID int64\n\n" +
		annotation +
		"OtherID int64\n}\n"
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	results := plan.DiffResults()
	c.Assert(results, qt.HasLen, 1)
	c.Assert(results[0].RemovedLines, qt.Equals, 2)
	c.Assert(strings.Count(results[0].Diff, "-"+annotation), qt.Equals, 2)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, original)
}

func TestCleanDir_HappyPath_PreservesStringLiteralsAndRemovesStandaloneAnnotations(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	original := "package models\n\n" +
		"const raw = `header\n" +
		"//migrator:schema:table name=\"raw_literal\"\n" +
		"//migrator:embedded mode=\"raw_literal\"\n" +
		"footer`\n\n" +
		"const interpreted = \"//migrator:schema:field name=\\\"interpreted_literal\\\" type=\\\"TEXT\\\"\"\n" +
		"const multiline = \"header\\n//migrator:embedded mode=\\\"interpreted_literal\\\"\\nfooter\"\n" +
		"const inline = 1 //migrator:schema:table name=\"inline_comment\"\n\n" +
		"//migrator:embeddedness is ordinary documentation.\n" +
		"//migrator:schema:not-a-known-directive is ordinary documentation.\n" +
		"// User is business documentation.\n" +
		"//migrator:schema:table name=\"users\"\n" +
		"type User struct {\n" +
		"\t//migrator:schema:field name=\"id\" type=\"BIGINT\"\n" +
		"\tID int64\n" +
		"\t//migrator:embedded mode=\"inline\"\n" +
		"\tTimestamps\n" +
		"}\n"
	expected := "package models\n\n" +
		"const raw = `header\n" +
		"//migrator:schema:table name=\"raw_literal\"\n" +
		"//migrator:embedded mode=\"raw_literal\"\n" +
		"footer`\n\n" +
		"const interpreted = \"//migrator:schema:field name=\\\"interpreted_literal\\\" type=\\\"TEXT\\\"\"\n" +
		"const multiline = \"header\\n//migrator:embedded mode=\\\"interpreted_literal\\\"\\nfooter\"\n" +
		"const inline = 1 //migrator:schema:table name=\"inline_comment\"\n\n" +
		"//migrator:embeddedness is ordinary documentation.\n" +
		"//migrator:schema:not-a-known-directive is ordinary documentation.\n" +
		"// User is business documentation.\n" +
		"type User struct {\n" +
		"\tID int64\n" +
		"\tTimestamps\n" +
		"}\n"
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	results := plan.Results()
	c.Assert(results, qt.HasLen, 1)
	c.Assert(results[0].Path, qt.Equals, path)
	c.Assert(results[0].Changed, qt.IsTrue)
	c.Assert(results[0].RemovedLines, qt.Equals, 3)
	c.Assert(results[0].Diff, qt.Equals, "")
	c.Assert(plan.Apply(), qt.IsNil)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, expected)
}

func TestCleanDir_HappyPath_SkipsTestVendorAndHiddenSources(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	testPath := filepath.Join(dir, "model_test.go")
	vendorDir := filepath.Join(dir, "vendor", "example")
	vendorPath := filepath.Join(vendorDir, "model.go")
	hiddenDir := filepath.Join(dir, "models", ".codex", "worktrees")
	hiddenPath := filepath.Join(hiddenDir, "model.go")
	original := "package models\n\n//migrator:schema:table name=\"users\" platform.mysql.engine=\"InnoDB\"\ntype User struct{}\n"
	expected := "package models\n\ntype User struct{}\n"
	skippedTest := "package models\n\n//migrator:schema:table name=\"test_only\"\ntype TestOnly struct {\n"
	skippedVendor := "package example\n\n//migrator:schema:table name=\"vendor_only\"\ntype VendorOnly struct {\n"
	skippedHidden := "package worktrees\n\n//migrator:schema:table name=\"hidden_only\"\ntype HiddenOnly struct {\n"
	c.Assert(os.MkdirAll(vendorDir, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(hiddenDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(testPath, []byte(skippedTest), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(vendorPath, []byte(skippedVendor), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(hiddenPath, []byte(skippedHidden), 0o600), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	results := plan.Results()
	c.Assert(results, qt.HasLen, 1)
	c.Assert(results[0].Path, qt.Equals, path)
	c.Assert(plan.Apply(), qt.IsNil)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, expected)
	testContent, err := os.ReadFile(testPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(testContent), qt.Equals, skippedTest)
	vendorContent, err := os.ReadFile(vendorPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(vendorContent), qt.Equals, skippedVendor)
	hiddenContent, err := os.ReadFile(hiddenPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(hiddenContent), qt.Equals, skippedHidden)
}

func TestCleanDir_FailurePath_InvalidGoSourceIsNotModified(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	validPath := filepath.Join(dir, "a_valid.go")
	invalidPath := filepath.Join(dir, "z_invalid.go")
	validOriginal := "package models\n\n//migrator:schema:table name=\"users\"\ntype User struct{}\n"
	invalidOriginal := "package models\n\n//migrator:schema:table name=\"broken\"\ntype Broken struct {\n"
	c.Assert(os.WriteFile(validPath, []byte(validOriginal), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(invalidPath, []byte(invalidOriginal), 0o600), qt.IsNil)
	c.Assert(os.Chmod(validPath, 0o640), qt.IsNil)
	c.Assert(os.Chmod(invalidPath, 0o640), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "z_invalid.go")
	c.Assert(plan, qt.IsNil)
	validContent, err := os.ReadFile(validPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(validContent), qt.Equals, validOriginal)
	invalidContent, err := os.ReadFile(invalidPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(invalidContent), qt.Equals, invalidOriginal)
	validInfo, err := os.Stat(validPath)
	c.Assert(err, qt.IsNil)
	c.Assert(validInfo.Mode().Perm(), qt.Equals, os.FileMode(0o640))
	invalidInfo, err := os.Stat(invalidPath)
	c.Assert(err, qt.IsNil)
	c.Assert(invalidInfo.Mode().Perm(), qt.Equals, os.FileMode(0o640))
}

func TestPlanApply_FailurePath_PrevalidatesEverySourceBeforeWriting(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "a_model.go")
	changedPath := filepath.Join(dir, "z_model.go")
	firstData := []byte("package models\n\n//migrator:schema:table name=\"first\"\ntype First struct{}\n")
	changedData := []byte("package models\n\n//migrator:schema:table name=\"changed\"\ntype Changed struct{}\n")
	replacementData := []byte("package models\n\ntype Changed struct{ ID int64 }\n")
	c.Assert(os.WriteFile(firstPath, firstData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(changedPath, changedData, 0o600), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(changedPath, replacementData, 0o600), qt.IsNil)

	err = plan.Apply()

	c.Assert(err, qt.ErrorMatches, "go source changed before cleanup: .*z_model.go")
	firstAfter, err := os.ReadFile(firstPath)
	c.Assert(err, qt.IsNil)
	c.Assert(firstAfter, qt.DeepEquals, firstData)
	changedAfter, err := os.ReadFile(changedPath)
	c.Assert(err, qt.IsNil)
	c.Assert(changedAfter, qt.DeepEquals, replacementData)
}

func TestCleanDir_HappyPath_DryRunDiffAndWriteAreConsistentAndIdempotent(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	original := "package models\r\n" +
		"\r\n" +
		"// User keeps business documentation.  \r\n" +
		"//migrator:schema:table name=\"users\"\r\n" +
		"type User struct{ID int64}\r\n" +
		"\t//migrator:embedded mode=\"inline\"\r\n" +
		"type Audit struct{CreatedAt int64}\r\n"
	expected := "package models\r\n" +
		"\r\n" +
		"// User keeps business documentation.  \r\n" +
		"type User struct{ID int64}\r\n" +
		"type Audit struct{CreatedAt int64}\r\n"
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)
	c.Assert(os.Chmod(path, 0o640), qt.IsNil)

	dryRunPlan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	dryRunResults := dryRunPlan.Results()
	c.Assert(dryRunResults, qt.HasLen, 1)
	c.Assert(dryRunResults[0].Path, qt.Equals, path)
	c.Assert(dryRunResults[0].Changed, qt.IsTrue)
	c.Assert(dryRunResults[0].RemovedLines, qt.Equals, 2)
	c.Assert(dryRunResults[0].Diff, qt.Equals, "")
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, original)
	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o640))

	diffPlan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	diffResults := diffPlan.DiffResults()
	c.Assert(diffResults, qt.HasLen, 1)
	c.Assert(diffResults[0].Path, qt.Equals, dryRunResults[0].Path)
	c.Assert(diffResults[0].Changed, qt.Equals, dryRunResults[0].Changed)
	c.Assert(diffResults[0].RemovedLines, qt.Equals, dryRunResults[0].RemovedLines)
	c.Assert(diffResults[0].Diff, qt.Contains, "-//migrator:schema:table name=\"users\"\r\n")
	c.Assert(diffResults[0].Diff, qt.Contains, "-\t//migrator:embedded mode=\"inline\"\r\n")
	content, err = os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, original)
	info, err = os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o640))

	writePlan, err := goannotationcleanup.PlanDir(dir)

	c.Assert(err, qt.IsNil)
	writeResults := writePlan.Results()
	c.Assert(writeResults, qt.HasLen, 1)
	c.Assert(writeResults[0].Path, qt.Equals, dryRunResults[0].Path)
	c.Assert(writeResults[0].Changed, qt.Equals, dryRunResults[0].Changed)
	c.Assert(writeResults[0].RemovedLines, qt.Equals, dryRunResults[0].RemovedLines)
	c.Assert(writeResults[0].Diff, qt.Equals, "")
	c.Assert(writePlan.Apply(), qt.IsNil)
	content, err = os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, expected)
	info, err = os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o640))

	idempotentDryRunPlan, err := goannotationcleanup.PlanDir(dir)
	c.Assert(err, qt.IsNil)
	idempotentDryRunResults := idempotentDryRunPlan.DiffResults()
	c.Assert(idempotentDryRunResults, qt.HasLen, 0)

	idempotentWritePlan, err := goannotationcleanup.PlanDir(dir)
	c.Assert(err, qt.IsNil)
	idempotentWriteResults := idempotentWritePlan.Results()
	c.Assert(idempotentWriteResults, qt.HasLen, 0)
	c.Assert(idempotentWritePlan.Apply(), qt.IsNil)
	content, err = os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, expected)
}

func TestCleanDir_HappyPath_DiffMarksMissingFinalNewline(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name         string
		original     string
		wantDiffLine string
	}{
		{
			name:         "removed final annotation",
			original:     "package models\n//migrator:schema:table name=\"users\"",
			wantDiffLine: "-//migrator:schema:table name=\"users\"\n",
		},
		{
			name:         "final context line",
			original:     "package models\n//migrator:schema:table name=\"users\"\ntype User struct{}",
			wantDiffLine: " type User struct{}\n",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			dir := c.TempDir()
			path := filepath.Join(dir, "model.go")
			c.Assert(os.WriteFile(path, []byte(tt.original), 0o600), qt.IsNil)

			plan, err := goannotationcleanup.PlanDir(dir)

			c.Assert(err, qt.IsNil)
			results := plan.DiffResults()
			c.Assert(results, qt.HasLen, 1)
			c.Assert(results[0].Diff, qt.Contains, tt.wantDiffLine)
			c.Assert(strings.Count(results[0].Diff, "\\ No newline at end of file\n"), qt.Equals, 1)
			content, err := os.ReadFile(path)
			c.Assert(err, qt.IsNil)
			c.Assert(string(content), qt.Equals, tt.original)
		})
	}
}

func TestPlanSourceAlias_HappyPath_ReportsExactSourceAndMissingPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "model.go")
	original := "package models\n\n//migrator:schema:table name=\"users\" platform.mysql.engine=\"InnoDB\"\ntype User struct{}\n"
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)

	plan, err := goannotationcleanup.PlanDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Annotations(), qt.DeepEquals, []goannotationcleanup.Annotation{
		{
			Path:       path,
			Line:       3,
			Directive:  "migrator:schema:table",
			Attributes: []string{"name", "platform.mysql.engine"},
		},
	})

	alias, err := plan.SourceAlias(path)
	c.Assert(err, qt.IsNil)
	c.Assert(alias, qt.Equals, path)

	alias, err = plan.SourceAlias(filepath.Join(dir, "schema.hcl"))
	c.Assert(err, qt.IsNil)
	c.Assert(alias, qt.Equals, "")
}
