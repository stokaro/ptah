package atlasreport

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/internal/migratesum"
	migrationlint "github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
)

type MigrateLintOptions struct {
	Driver    string
	URL       string
	Dir       string
	FS        fs.FS
	Findings  []migrationlint.Finding
	Versions  []int64
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
	Name     string                  `json:"Name,omitempty"`
	Text     string                  `json:"Text,omitempty"`
	Error    string                  `json:"Error,omitempty"`
	Findings []migrationlint.Finding `json:"Findings,omitempty"`
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
		result.Steps = migrateLintSteps(nil, opts.Integrity, "")
		result.Files = []MigrateLintFile{
			{
				Name:  migratesum.AtlasFileName,
				Error: opts.Integrity.Error,
			},
		}
		return result, nil
	}

	files, err := migrateLintFiles(opts.FS)
	if err != nil {
		return MigrateLint{}, err
	}
	files = migrateLintSelectedFiles(files, opts.Versions)
	files = attachMigrateLintFindings(files, opts.Findings)
	result.Steps = migrateLintSteps(files, opts.Integrity, opts.Error)
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

func migrateLintFiles(fsys fs.FS) ([]MigrateLintFile, error) {
	discovered, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return nil, fmt.Errorf("discover Atlas migration files: %w", err)
	}
	files := make([]MigrateLintFile, 0, len(discovered))
	for _, file := range discovered {
		if file.Repeatable || file.Direction == "down" {
			continue
		}
		raw, err := fs.ReadFile(fsys, file.Path)
		if err != nil {
			return nil, fmt.Errorf("read migration file %s: %w", file.Path, err)
		}
		files = append(files, MigrateLintFile{
			Name: file.Path,
			Text: string(raw),
		})
	}
	return files, nil
}

func attachMigrateLintFindings(
	files []MigrateLintFile,
	findings []migrationlint.Finding,
) []MigrateLintFile {
	for i := range files {
		name := files[i].Name
		for _, finding := range findings {
			if sameMigrateLintFile(name, finding.File) {
				files[i].Findings = append(files[i].Findings, finding)
			}
		}
	}
	return files
}

func sameMigrateLintFile(name, findingPath string) bool {
	if findingPath == "" {
		return false
	}
	return path.Base(findingPath) == path.Base(name) ||
		strings.TrimPrefix(path.Clean(findingPath), "./") == strings.TrimPrefix(path.Clean(name), "./")
}

func migrateLintSteps(files []MigrateLintFile, integrity MigrateLintIntegrity, errText string) []MigrateLintStep {
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
		Text: fmt.Sprintf("Found %d new migration files (from %d total)", len(files), len(files)),
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

func migrateLintSelectedFiles(files []MigrateLintFile, versions []int64) []MigrateLintFile {
	if len(versions) == 0 {
		return files
	}
	out := make([]MigrateLintFile, 0, len(files))
	for _, file := range files {
		version := migrateLintVersion(file.Name)
		if slices.Contains(versions, version) {
			out = append(out, file)
		}
	}
	return out
}

func migrateLintVersion(name string) int64 {
	parsed, err := migrator.ParseAtlasMigrationFileName(path.Base(name))
	if err != nil {
		return 0
	}
	return parsed.Version
}
