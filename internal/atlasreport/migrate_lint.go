package atlasreport

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/internal/atlaslint"
	"github.com/stokaro/ptah/internal/migratesum"
	migrationlint "github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
)

type MigrateLintOptions struct {
	Driver    string
	URL       string
	Dir       string
	Analysis  *migrationlint.Analysis
	Integrity MigrateLintIntegrity
	Error     string
}

type MigrateLintIntegrity struct {
	Checked bool
	Error   string
}

type MigrateLint struct {
	Env   atlasEnv          `json:"Env"`
	Steps []MigrateLintStep `json:"Steps,omitempty"`
	Files []MigrateLintFile `json:"Files,omitempty"`
}

type MigrateLintStep struct {
	Name   string           `json:"Name,omitempty"`
	Text   string           `json:"Text,omitempty"`
	Error  string           `json:"Error,omitempty"`
	Result *MigrateLintFile `json:"Result,omitempty"`
}

type MigrateLintFile struct {
	Name       string                  `json:"Name,omitempty"`
	Text       string                  `json:"Text,omitempty"`
	Error      string                  `json:"Error,omitempty"`
	Findings   []migrationlint.Finding `json:"Findings,omitempty"`
	sourcePath string
}

func WriteMigrateLintFormat(w io.Writer, format string, opts MigrateLintOptions) error {
	result, err := NewMigrateLint(opts)
	if err != nil {
		return err
	}
	return renderAtlasGoTemplate(w, "atlas-migrate-lint-format", format, result)
}

func ValidateMigrateLintTemplate(format string) error {
	return validateAtlasGoTemplate("atlas-migrate-lint-format", format)
}

func NewMigrateLint(opts MigrateLintOptions) (MigrateLint, error) {
	result := MigrateLint{
		Env: atlasEnv{
			Driver: opts.Driver,
			URL:    atlasRedactedURL(opts.URL),
			Dir:    opts.Dir,
		},
	}
	if opts.Integrity.Failed() {
		result.Steps = migrateLintSteps(nil, 0, opts.Integrity, "")
		result.Files = []MigrateLintFile{
			{
				Name:  migratesum.AtlasFileName,
				Error: opts.Integrity.Error,
			},
		}
		return result, nil
	}

	if opts.Analysis == nil {
		return MigrateLint{}, fmt.Errorf("migration lint analysis is required")
	}
	files, total := migrateLintFiles(*opts.Analysis)
	files = attachMigrateLintFindings(files, opts.Analysis.Findings())
	result.Steps = migrateLintSteps(files, total, opts.Integrity, opts.Error)
	result.Files = files
	return result, nil
}

func InspectMigrateLintIntegrity(fsys fs.FS) (MigrateLintIntegrity, error) {
	_, err := fs.Stat(fsys, migratesum.AtlasFileName)
	if errors.Is(err, fs.ErrNotExist) {
		return MigrateLintIntegrity{}, nil
	}
	if err != nil {
		return MigrateLintIntegrity{}, fmt.Errorf("stat %s: %w", migratesum.AtlasFileName, err)
	}
	result, err := migratesum.VerifyWithFormat(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return MigrateLintIntegrity{Checked: true, Error: "checksum mismatch"}, nil
	}
	if !result.OK() {
		return MigrateLintIntegrity{Checked: true, Error: "checksum mismatch"}, nil
	}
	return MigrateLintIntegrity{Checked: true}, nil
}

func (i MigrateLintIntegrity) Failed() bool {
	return i.Checked && i.Error != ""
}

func migrateLintFiles(analysis migrationlint.Analysis) ([]MigrateLintFile, int) {
	prepared := analysis.Files()
	slices.SortStableFunc(prepared, func(a, b migrationlint.File) int {
		return cmp.Or(cmp.Compare(a.Version, b.Version), strings.Compare(a.Name, b.Name))
	})
	files := make([]MigrateLintFile, 0, len(prepared))
	total := 0
	for _, file := range prepared {
		if file.Repeatable || file.Direction != "up" || file.Ignored {
			continue
		}
		total++
		if !file.Selected {
			continue
		}
		files = append(files, MigrateLintFile{
			Name:       file.Name,
			Text:       file.Source,
			sourcePath: file.Path,
		})
	}
	return files, total
}

func attachMigrateLintFindings(
	files []MigrateLintFile,
	findings []migrationlint.Finding,
) []MigrateLintFile {
	for _, finding := range findings {
		for i := range files {
			if sameMigrateLintFile(files[i].sourcePath, finding.File) {
				files[i].Findings = append(files[i].Findings, atlasMigrateLintFinding(finding))
			}
		}
	}
	return files
}

func sameMigrateLintFile(sourcePath, findingPath string) bool {
	if findingPath == "" {
		return false
	}
	cleanFinding := strings.TrimPrefix(path.Clean(findingPath), "./")
	return cleanFinding == strings.TrimPrefix(path.Clean(sourcePath), "./")
}

func atlasMigrateLintFinding(finding migrationlint.Finding) migrationlint.Finding {
	finding.Rule = atlaslint.RuleForNativeCode(finding.Rule).Code
	return finding
}

func migrateLintSteps(
	files []MigrateLintFile,
	total int,
	integrity MigrateLintIntegrity,
	errText string,
) []MigrateLintStep {
	steps := make([]MigrateLintStep, 0, len(files)+3)
	if integrity.Checked {
		step := MigrateLintStep{
			Name: "Migration Integrity Check",
			Text: "File atlas.sum is valid",
		}
		if integrity.Failed() {
			step.Text = "File atlas.sum is invalid"
			step.Error = integrity.Error
			return append(steps, step)
		}
		steps = append(steps, step)
	}
	steps = append(steps, MigrateLintStep{
		Name: "Detect New Migration Files",
		Text: fmt.Sprintf("Found %d new migration files (from %d total)", len(files), total),
	})
	if errText != "" {
		return append(steps, MigrateLintStep{
			Name:  "Replay Migration Files",
			Text:  "Failed loading changes on dev database",
			Error: errText,
		})
	}
	steps = append(steps, MigrateLintStep{
		Name: "Replay Migration Files",
		Text: fmt.Sprintf("Loaded %d changes on dev database", len(files)),
	})
	for _, file := range files {
		steps = append(steps, MigrateLintStep{
			Name:   "Analyze " + file.Name,
			Text:   strconv.Itoa(len(file.Findings)) + " reports were found in analysis",
			Result: &file,
		})
	}
	return steps
}
