// Package goannotationexport migrates Ptah Go annotations to HCL schema files.
package goannotationexport

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/atlashcl"
	"github.com/stokaro/ptah/internal/atlashclrender"
	"github.com/stokaro/ptah/internal/goannotationcleanup"
	"github.com/stokaro/ptah/internal/goannotationsource"
	"github.com/stokaro/ptah/internal/pathguard"
)

var (
	// ErrNoAnnotations reports that destructive cleanup has no source annotations
	// to migrate and would therefore risk replacing a previous export with an
	// empty schema.
	ErrNoAnnotations = errors.New("no Ptah Go annotations found to export and clean")
	// ErrLossyCleanup reports that destructive cleanup would discard schema intent.
	ErrLossyCleanup = errors.New("refuse to clean Go annotations after a lossy HCL export")
	// ErrInvalidHCL reports that generated HCL is not parseable and stable.
	ErrInvalidHCL = errors.New("generated HCL failed round-trip validation")
	// ErrOutputAliasesSource reports that the output aliases an input Go source.
	ErrOutputAliasesSource = errors.New("HCL output aliases a Go source file")
	// ErrOutputIsGoSource reports that the HCL destination uses a Go source path.
	ErrOutputIsGoSource = errors.New("HCL output path must not end in .go")
	// ErrOutputAliasesManagedData reports that the output aliases a managed-data
	// source and would overwrite the referenced row data.
	ErrOutputAliasesManagedData = errors.New("HCL output aliases a managed data file")
)

// Options controls one Go-annotation-to-HCL export.
type Options struct {
	RootDir    string
	OutputPath string
	Cleanup    bool
	DryRun     bool
	Diff       bool
}

// Result describes a completed export.
type Result struct {
	OutputPath   string
	Tables       int
	Fields       int
	Enums        int
	Diagnostics  []atlashclrender.Diagnostic
	Cleanup      []goannotationcleanup.Result
	RemovedLines int
}

type exportPlan struct {
	options        Options
	outputPath     string
	snapshot       *goannotationsource.Snapshot
	cleanup        *goannotationcleanup.Plan
	cleanupResults []goannotationcleanup.Result
}

type renderedExport struct {
	database    *goschema.Database
	hcl         []byte
	diagnostics []atlashclrender.Diagnostic
}

type stagedOutput struct {
	outputPath string
	tempPath   string
}

// Export renders Go annotations to HCL and optionally applies one validated
// cleanup plan after the output has been committed.
func Export(opts Options) (Result, error) {
	return export(opts, func() {})
}

func export(opts Options, afterOutputStage func()) (Result, error) {
	plan, err := prepareExport(opts)
	if err != nil {
		return Result{}, err
	}
	rendered, err := renderExport(plan)
	if err != nil {
		return Result{}, err
	}
	mode, err := outputMode(plan.outputPath, rendered.database.Roles)
	if err != nil {
		return Result{}, err
	}
	staged, err := stageOutput(plan.outputPath, rendered.hcl, mode)
	if err != nil {
		return Result{}, err
	}
	afterOutputStage()
	if err := plan.validatePublication(rendered.database.ManagedData); err != nil {
		return Result{}, errors.Join(err, staged.cleanup())
	}
	if err := staged.commit(); err != nil {
		return Result{}, errors.Join(err, staged.cleanup())
	}
	if err := plan.applyCleanup(); err != nil {
		return Result{}, err
	}

	return Result{
		OutputPath:   plan.outputPath,
		Tables:       len(rendered.database.Tables),
		Fields:       len(rendered.database.Fields),
		Enums:        len(rendered.database.Enums),
		Diagnostics:  rendered.diagnostics,
		Cleanup:      plan.cleanupResults,
		RemovedLines: removedAnnotationLines(plan.cleanupResults),
	}, nil
}

func prepareExport(opts Options) (exportPlan, error) {
	if (opts.DryRun || opts.Diff) && !opts.Cleanup {
		return exportPlan{}, fmt.Errorf("cleanup dry-run and diff require cleanup")
	}
	rootDir, outputPath, err := resolvePaths(opts)
	if err != nil {
		return exportPlan{}, err
	}

	snapshot, err := goannotationsource.Capture(rootDir)
	if err != nil {
		return exportPlan{}, fmt.Errorf("capture Go annotation sources: %w", err)
	}
	cleanupPlan, err := goannotationcleanup.NewPlan(snapshot)
	if err != nil {
		return exportPlan{}, fmt.Errorf("plan Go annotation cleanup: %w", err)
	}
	if alias, err := snapshot.SourceAlias(outputPath); err != nil {
		return exportPlan{}, err
	} else if alias != "" {
		return exportPlan{}, fmt.Errorf("%w: output %s refers to %s", ErrOutputAliasesSource, outputPath, alias)
	}
	if strings.HasSuffix(outputPath, ".go") {
		return exportPlan{}, fmt.Errorf("%w: %s", ErrOutputIsGoSource, outputPath)
	}

	cleanupResults := cleanupPlan.Results()
	if opts.Diff {
		cleanupResults = cleanupPlan.DiffResults()
	}
	if opts.Cleanup && len(cleanupResults) == 0 {
		return exportPlan{}, ErrNoAnnotations
	}

	return exportPlan{
		options:        opts,
		outputPath:     outputPath,
		snapshot:       snapshot,
		cleanup:        cleanupPlan,
		cleanupResults: cleanupResults,
	}, nil
}

func renderExport(plan exportPlan) (renderedExport, error) {
	db, err := goschema.ParseFS(plan.snapshot.FS(), ".")
	if err != nil {
		return renderedExport{}, fmt.Errorf("parse Go annotations: %w", err)
	}
	bindSnapshotManagedDataSourceRoot(db.ManagedData, plan.snapshot.Root())
	if alias, err := managedDataSourceAlias(db.ManagedData, plan.outputPath); err != nil {
		return renderedExport{}, err
	} else if alias != "" {
		return renderedExport{}, fmt.Errorf(
			"%w: output %s refers to %s",
			ErrOutputAliasesManagedData,
			plan.outputPath,
			alias,
		)
	}
	if err := rebaseManagedDataFiles(db.ManagedData, plan.outputPath); err != nil {
		return renderedExport{}, err
	}
	rendered, err := atlashclrender.Render(db)
	if err != nil {
		return renderedExport{}, fmt.Errorf("render HCL schema: %w", err)
	}
	diagnostics := append([]atlashclrender.Diagnostic(nil), rendered.Diagnostics...)
	sortDiagnostics(diagnostics)
	canonicalHCL, err := canonicalRoundTrip(rendered.Data)
	if err != nil {
		return renderedExport{}, err
	}
	if plan.options.Cleanup && len(diagnostics) > 0 {
		return renderedExport{}, lossyCleanupError(diagnostics)
	}
	return renderedExport{
		database:    db,
		hcl:         canonicalHCL,
		diagnostics: diagnostics,
	}, nil
}

func bindSnapshotManagedDataSourceRoot(values []goschema.ManagedData, root string) {
	for i := range values {
		if filepath.IsAbs(values[i].SourceDir) {
			continue
		}
		values[i].SourceDir = filepath.Clean(
			filepath.Join(root, filepath.FromSlash(values[i].SourceDir)),
		)
	}
}

func (p exportPlan) validatePublication(managedData []goschema.ManagedData) error {
	if err := p.snapshot.Revalidate(); err != nil {
		return fmt.Errorf("revalidate Go annotation sources before HCL publication: %w", err)
	}
	alias, err := p.snapshot.SourceAlias(p.outputPath)
	if err != nil {
		return err
	}
	if alias != "" {
		return fmt.Errorf(
			"%w: output %s refers to %s",
			ErrOutputAliasesSource,
			p.outputPath,
			alias,
		)
	}
	if alias, err := managedDataSourceAlias(managedData, p.outputPath); err != nil {
		return err
	} else if alias != "" {
		return fmt.Errorf(
			"%w: output %s refers to %s",
			ErrOutputAliasesManagedData,
			p.outputPath,
			alias,
		)
	}
	return nil
}

func managedDataSourceAlias(values []goschema.ManagedData, outputPath string) (string, error) {
	outputInfo, err := os.Stat(outputPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("stat HCL output %s: %w", outputPath, err)
	}

	for _, value := range values {
		sourcePath := value.File
		if !filepath.IsAbs(sourcePath) {
			sourcePath = filepath.Join(value.SourceDir, sourcePath)
		}
		resolvedSource, err := pathguard.ResolveWithinRoot(sourcePath, "")
		if err != nil {
			return "", fmt.Errorf("resolve managed data file %q: %w", value.File, err)
		}
		if resolvedSource == outputPath {
			return resolvedSource, nil
		}
		if outputInfo == nil {
			continue
		}
		sourceInfo, err := os.Stat(resolvedSource)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("stat managed data file %s: %w", resolvedSource, err)
		}
		if os.SameFile(outputInfo, sourceInfo) {
			return resolvedSource, nil
		}
	}
	return "", nil
}

func rebaseManagedDataFiles(values []goschema.ManagedData, outputPath string) error {
	outputDir := filepath.Dir(outputPath)
	for i := range values {
		sourcePath := values[i].File
		if !filepath.IsAbs(sourcePath) {
			sourcePath = filepath.Join(values[i].SourceDir, sourcePath)
		}
		relativePath, err := filepath.Rel(outputDir, sourcePath)
		if err != nil {
			return fmt.Errorf("rebase managed data file %q: %w", values[i].File, err)
		}
		values[i].File = filepath.ToSlash(relativePath)
		values[i].SourceDir = outputDir
	}
	return nil
}

func (p exportPlan) applyCleanup() error {
	if !p.options.Cleanup || p.options.DryRun || p.options.Diff {
		return nil
	}
	if err := p.cleanup.Apply(); err != nil {
		return fmt.Errorf("apply Go annotation cleanup: %w", err)
	}
	return nil
}

func removedAnnotationLines(results []goannotationcleanup.Result) int {
	total := 0
	for _, result := range results {
		total += result.RemovedLines
	}
	return total
}

func resolvePaths(opts Options) (rootDir string, outputPath string, err error) {
	root := opts.RootDir
	if strings.TrimSpace(root) == "" {
		root = "."
	}
	rootDir, err = pathguard.ResolveCLIPath(root)
	if err != nil {
		return "", "", fmt.Errorf("invalid root directory: %w", err)
	}
	info, err := os.Stat(rootDir)
	if err != nil {
		return "", "", fmt.Errorf("stat root directory: %w", err)
	}
	if !info.IsDir() {
		return "", "", fmt.Errorf("root path is not a directory: %s", rootDir)
	}
	outputPath, err = pathguard.ResolveCLIPath(opts.OutputPath)
	if err != nil {
		return "", "", fmt.Errorf("invalid output path: %w", err)
	}
	return rootDir, outputPath, nil
}

func canonicalRoundTrip(data []byte) ([]byte, error) {
	parsed, err := atlashcl.Parse(data, "schema.hcl")
	if err != nil {
		return nil, fmt.Errorf("%w: parse generated schema: %v", ErrInvalidHCL, err)
	}
	canonical, err := atlashclrender.Render(parsed)
	if err != nil {
		return nil, fmt.Errorf("%w: render canonical schema: %v", ErrInvalidHCL, err)
	}
	if len(canonical.Diagnostics) > 0 {
		return nil, ErrInvalidHCL
	}
	if !bytes.Equal(canonical.Data, data) {
		return nil, fmt.Errorf("%w: canonical render changed the generated schema", ErrInvalidHCL)
	}
	reparsed, err := atlashcl.Parse(canonical.Data, "schema.hcl")
	if err != nil {
		return nil, fmt.Errorf("%w: parse canonical schema: %v", ErrInvalidHCL, err)
	}
	stable, err := atlashclrender.Render(reparsed)
	if err != nil {
		return nil, fmt.Errorf("%w: verify canonical schema: %v", ErrInvalidHCL, err)
	}
	if len(stable.Diagnostics) > 0 || !bytes.Equal(stable.Data, canonical.Data) {
		return nil, ErrInvalidHCL
	}
	return canonical.Data, nil
}

func sortDiagnostics(diagnostics []atlashclrender.Diagnostic) {
	sort.SliceStable(diagnostics, func(i, j int) bool {
		if diagnostics[i].Path != diagnostics[j].Path {
			return diagnostics[i].Path < diagnostics[j].Path
		}
		return diagnostics[i].Message < diagnostics[j].Message
	})
}

func lossyCleanupError(diagnostics []atlashclrender.Diagnostic) error {
	var builder strings.Builder
	for _, diagnostic := range diagnostics {
		fmt.Fprintf(&builder, "\n- %s: %s", diagnostic.Path, diagnostic.Message)
	}
	return fmt.Errorf("%w:%s", ErrLossyCleanup, builder.String())
}

func hasRolePasswords(roles []goschema.Role) bool {
	return slices.ContainsFunc(roles, func(role goschema.Role) bool {
		return role.Password != ""
	})
}

func stageOutput(path string, data []byte, mode os.FileMode) (stagedOutput, error) {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return stagedOutput{}, fmt.Errorf("create HCL output directory: %w", err)
	}

	file, err := os.CreateTemp(parent, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return stagedOutput{}, fmt.Errorf("create temporary HCL output: %w", err)
	}
	tempPath := file.Name()
	staged := stagedOutput{outputPath: path, tempPath: tempPath}

	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		return stagedOutput{}, errors.Join(
			fmt.Errorf("set temporary HCL output mode: %w", err),
			staged.cleanup(),
		)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return stagedOutput{}, errors.Join(
			fmt.Errorf("write temporary HCL output: %w", err),
			staged.cleanup(),
		)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return stagedOutput{}, errors.Join(
			fmt.Errorf("sync temporary HCL output: %w", err),
			staged.cleanup(),
		)
	}
	if err := file.Close(); err != nil {
		return stagedOutput{}, errors.Join(
			fmt.Errorf("close temporary HCL output: %w", err),
			staged.cleanup(),
		)
	}
	return staged, nil
}

func (s *stagedOutput) commit() error {
	if err := os.Rename(s.tempPath, s.outputPath); err != nil {
		return fmt.Errorf("commit HCL output: %w", err)
	}
	s.tempPath = ""
	return nil
}

func (s *stagedOutput) cleanup() error {
	if s.tempPath == "" {
		return nil
	}
	err := os.Remove(s.tempPath)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return fmt.Errorf("remove temporary HCL output: %w", err)
	}
	s.tempPath = ""
	return nil
}

func outputMode(path string, roles []goschema.Role) (os.FileMode, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0o600, nil
	}
	if err != nil {
		return 0, fmt.Errorf("stat HCL output: %w", err)
	}
	if !info.Mode().IsRegular() {
		return 0, fmt.Errorf("HCL output is not a regular file: %s", path)
	}
	if hasRolePasswords(roles) {
		return 0o600, nil
	}
	return info.Mode().Perm(), nil
}
