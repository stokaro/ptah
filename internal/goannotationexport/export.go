// Package goannotationexport migrates Ptah Go annotations to HCL schema files.
package goannotationexport

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/atlashcl"
	"github.com/stokaro/ptah/internal/atlashclrender"
	"github.com/stokaro/ptah/internal/fsdurable"
	"github.com/stokaro/ptah/internal/goannotationcleanup"
	"github.com/stokaro/ptah/internal/goannotationsource"
	"github.com/stokaro/ptah/internal/pathguard"
)

var (
	// ErrNoAnnotations reports that the source set has no annotations to export.
	// Refusing the export protects an existing HCL schema from empty replacement.
	ErrNoAnnotations = errors.New("no Ptah Go annotations found to export")
	// ErrNoExportableSchema reports that parsed annotations did not produce any
	// HCL schema object. Refusing the export protects an existing HCL schema from
	// header-only replacement.
	ErrNoExportableSchema = errors.New("Go annotations produced no exportable HCL schema objects")
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
	// ErrOutputChanged reports that another writer changed the HCL destination
	// after Ptah staged its replacement.
	ErrOutputChanged = errors.New("HCL output changed after staging")
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

type outputState struct {
	exists bool
	info   fs.FileInfo
	data   []byte
}

type stagedOutput struct {
	outputPath string
	parent     *pathguard.OpenedDirectory
	targetName string
	tempName   string
	tempInfo   fs.FileInfo
	mode       os.FileMode
	original   outputState
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
	staged, err := stageOutput(plan.outputPath, rendered.hcl, rendered.database.Roles)
	if err != nil {
		return Result{}, err
	}
	afterOutputStage()
	if err := plan.validatePublication(rendered.database.ManagedData); err != nil {
		return Result{}, errors.Join(err, staged.cleanup())
	}
	if err := staged.validateDestination(); err != nil {
		return Result{}, errors.Join(err, staged.cleanup())
	}
	if err := staged.commit(); err != nil {
		return Result{}, errors.Join(err, staged.cleanup())
	}
	if err := staged.close(); err != nil {
		return Result{}, fmt.Errorf(
			"%w: close HCL output directory after commit: %w",
			fsdurable.ErrReplacementCommitted,
			err,
		)
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
	if !databaseHasSchemaObjects(db) {
		return renderedExport{}, ErrNoAnnotations
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
	diagnostics = append(diagnostics, opaqueSQLDiagnostics(db, diagnostics)...)
	// Normalization loss is detected against the SOURCE, not the rendered HCL:
	// once cty has composed a value, the original code points are gone and the
	// round-trip below can only prove the composed form is self-stable.
	normalization, err := normalizationDiagnostics(plan.snapshot.FS(), rendered.Data)
	if err != nil {
		return renderedExport{}, fmt.Errorf("scan annotations for normalization loss: %w", err)
	}
	diagnostics = append(diagnostics, normalization...)
	sortDiagnostics(diagnostics)
	canonicalHCL, exportedDB, err := canonicalRoundTrip(rendered.Data)
	if err != nil {
		return renderedExport{}, err
	}
	if plan.options.Cleanup && len(diagnostics) > 0 {
		return renderedExport{}, lossyCleanupError(diagnostics)
	}
	if !databaseHasSchemaObjects(exportedDB) {
		return renderedExport{}, ErrNoExportableSchema
	}
	return renderedExport{
		database:    db,
		hcl:         canonicalHCL,
		diagnostics: diagnostics,
	}, nil
}

func databaseHasSchemaObjects(db *goschema.Database) bool {
	objectCount := len(db.Schemas) +
		len(db.Tables) +
		len(db.Fields) +
		len(db.Indexes) +
		len(db.Constraints) +
		len(db.Enums) +
		len(db.EmbeddedFields) +
		len(db.Extensions) +
		len(db.Functions) +
		len(db.Sequences) +
		len(db.Domains) +
		len(db.CompositeTypes) +
		len(db.Ranges) +
		len(db.Views) +
		len(db.MaterializedViews) +
		len(db.Triggers) +
		len(db.RLSPolicies) +
		len(db.RLSEnabledTables) +
		len(db.Roles) +
		len(db.Grants) +
		len(db.ManagedData)
	return objectCount > 0
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

func canonicalRoundTrip(data []byte) ([]byte, *goschema.Database, error) {
	parsed, err := atlashcl.Parse(data, "schema.hcl")
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse generated schema: %v", ErrInvalidHCL, err)
	}
	canonical, err := atlashclrender.Render(parsed)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: render canonical schema: %v", ErrInvalidHCL, err)
	}
	if len(canonical.Diagnostics) > 0 {
		return nil, nil, ErrInvalidHCL
	}
	if !bytes.Equal(canonical.Data, data) {
		return nil, nil, fmt.Errorf("%w: canonical render changed the generated schema", ErrInvalidHCL)
	}
	reparsed, err := atlashcl.Parse(canonical.Data, "schema.hcl")
	if err != nil {
		return nil, nil, fmt.Errorf("%w: parse canonical schema: %v", ErrInvalidHCL, err)
	}
	stable, err := atlashclrender.Render(reparsed)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: verify canonical schema: %v", ErrInvalidHCL, err)
	}
	if len(stable.Diagnostics) > 0 || !bytes.Equal(stable.Data, canonical.Data) {
		return nil, nil, ErrInvalidHCL
	}
	return canonical.Data, parsed, nil
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

func stageOutput(path string, data []byte, roles []goschema.Role) (stagedOutput, error) {
	parentPath := filepath.Dir(path)
	if err := os.MkdirAll(parentPath, 0o755); err != nil {
		return stagedOutput{}, fmt.Errorf("create HCL output directory: %w", err)
	}
	parent, err := pathguard.OpenDirectory(parentPath)
	if err != nil {
		return stagedOutput{}, fmt.Errorf("open HCL output directory: %w", err)
	}
	targetName := filepath.Base(path)
	original, mode, err := captureOutputState(parent, targetName, path, roles)
	if err != nil {
		return stagedOutput{}, errors.Join(err, parent.Close())
	}
	file, tempName, err := parent.CreateTemp("." + targetName + ".tmp-*")
	if err != nil {
		return stagedOutput{}, errors.Join(
			fmt.Errorf("create temporary HCL output: %w", err),
			parent.Close(),
		)
	}
	staged := stagedOutput{
		outputPath: path,
		parent:     parent,
		targetName: targetName,
		tempName:   tempName,
		mode:       mode,
		original:   original,
	}
	if _, err := file.Write(data); err != nil {
		return stagedOutput{}, errors.Join(
			fmt.Errorf("write temporary HCL output: %w", err),
			file.Close(),
			staged.cleanup(),
		)
	}
	if err := file.Sync(); err != nil {
		return stagedOutput{}, errors.Join(
			fmt.Errorf("sync temporary HCL output: %w", err),
			file.Close(),
			staged.cleanup(),
		)
	}
	info, err := file.Stat()
	if err != nil {
		return stagedOutput{}, errors.Join(
			fmt.Errorf("stat temporary HCL output: %w", err),
			file.Close(),
			staged.cleanup(),
		)
	}
	if err := file.Close(); err != nil {
		return stagedOutput{}, errors.Join(
			fmt.Errorf("close temporary HCL output: %w", err),
			staged.cleanup(),
		)
	}
	staged.tempInfo = info
	if err := parent.Sync(); err != nil {
		return stagedOutput{}, errors.Join(
			fmt.Errorf("sync temporary HCL output directory: %w", err),
			staged.cleanup(),
		)
	}
	return staged, nil
}

func captureOutputState(
	parent *pathguard.OpenedDirectory,
	targetName, outputPath string,
	roles []goschema.Role,
) (outputState, os.FileMode, error) {
	info, data, err := readOutputFile(parent, targetName, outputPath)
	if errors.Is(err, os.ErrNotExist) {
		return outputState{}, 0o600, nil
	}
	if err != nil {
		return outputState{}, 0, err
	}
	mode := info.Mode().Perm()
	if hasRolePasswords(roles) {
		mode = 0o600
	}
	return outputState{exists: true, info: info, data: data}, mode, nil
}

func readOutputFile(
	parent *pathguard.OpenedDirectory,
	targetName, outputPath string,
) (fs.FileInfo, []byte, error) {
	entryInfo, err := parent.Lstat(targetName)
	if err != nil {
		return nil, nil, fmt.Errorf("stat HCL output %s: %w", outputPath, err)
	}
	if !entryInfo.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("HCL output is not a regular file: %s", outputPath)
	}
	file, err := parent.Open(targetName)
	if err != nil {
		return nil, nil, fmt.Errorf("open HCL output %s: %w", outputPath, err)
	}
	info, statErr := file.Stat()
	openedEntryInfo, restatErr := parent.Lstat(targetName)
	validationErr := errors.Join(statErr, restatErr)
	if validationErr == nil {
		validationErr = validateOpenedOutputFile(outputPath, entryInfo, info, openedEntryInfo)
	}
	var data []byte
	readErr := error(nil)
	if validationErr == nil {
		data, readErr = io.ReadAll(file)
	}
	finalInfo, finalStatErr := file.Stat()
	finalEntryInfo, finalRestatErr := parent.Lstat(targetName)
	finalValidationErr := errors.Join(finalStatErr, finalRestatErr)
	if finalValidationErr == nil && validationErr == nil {
		finalValidationErr = validateReadOutputFile(outputPath, info, finalInfo, finalEntryInfo)
	}
	closeErr := file.Close()
	if err := errors.Join(
		validationErr,
		readErr,
		finalValidationErr,
		closeErr,
	); err != nil {
		return nil, nil, err
	}
	return finalInfo, data, nil
}

func validateOpenedOutputFile(
	outputPath string,
	entryInfo, info, openedEntryInfo fs.FileInfo,
) error {
	if !info.Mode().IsRegular() ||
		!openedEntryInfo.Mode().IsRegular() ||
		!os.SameFile(entryInfo, info) ||
		!os.SameFile(info, openedEntryInfo) {
		return fmt.Errorf("%w: %s", ErrOutputChanged, outputPath)
	}
	return nil
}

func validateReadOutputFile(
	outputPath string,
	info, finalInfo, finalEntryInfo fs.FileInfo,
) error {
	if !finalInfo.Mode().IsRegular() ||
		!finalEntryInfo.Mode().IsRegular() ||
		info.Mode() != finalInfo.Mode() ||
		finalInfo.Mode() != finalEntryInfo.Mode() ||
		info.Size() != finalInfo.Size() ||
		!info.ModTime().Equal(finalInfo.ModTime()) ||
		!os.SameFile(info, finalInfo) ||
		!os.SameFile(finalInfo, finalEntryInfo) {
		return fmt.Errorf("%w: %s", ErrOutputChanged, outputPath)
	}
	return nil
}

func (s *stagedOutput) validateDestination() error {
	if err := s.parent.Revalidate(); err != nil {
		return fmt.Errorf("%w: revalidate output parent %s: %w", ErrOutputChanged, s.outputPath, err)
	}
	info, data, err := readOutputFile(s.parent, s.targetName, s.outputPath)
	if !s.original.exists && errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrOutputChanged, s.outputPath, err)
	}
	if !s.original.exists ||
		!os.SameFile(s.original.info, info) ||
		s.original.info.Mode() != info.Mode() ||
		!bytes.Equal(s.original.data, data) {
		return fmt.Errorf("%w: %s", ErrOutputChanged, s.outputPath)
	}
	return nil
}

func (s *stagedOutput) commit() error {
	if err := s.validateDestination(); err != nil {
		return err
	}
	if err := s.parent.Revalidate(); err != nil {
		return fmt.Errorf(
			"%w: revalidate output parent before commit: %w",
			ErrOutputChanged,
			err,
		)
	}
	replaceErr := s.parent.PublishFile(s.tempName, s.targetName, s.tempInfo, s.mode)
	committed := replaceErr == nil || errors.Is(replaceErr, fsdurable.ErrReplacementCommitted)
	if !committed {
		return fmt.Errorf("commit HCL output: %w", replaceErr)
	}
	s.tempName = ""
	commitErr := errors.Join(replaceErr, s.parent.Revalidate())
	if commitErr == nil {
		return nil
	}
	if !errors.Is(commitErr, fsdurable.ErrReplacementCommitted) {
		commitErr = fmt.Errorf("%w: %w", fsdurable.ErrReplacementCommitted, commitErr)
	}
	return fmt.Errorf("commit HCL output: %w", commitErr)
}

func (s *stagedOutput) cleanup() error {
	removeErr := error(nil)
	syncErr := error(nil)
	if s.tempName != "" {
		removeErr = s.parent.Remove(s.tempName)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
		if removeErr == nil {
			s.tempName = ""
			syncErr = s.parent.Sync()
		}
	}
	closeParentErr := s.closeParent()
	return errors.Join(
		wrapOutputError("remove temporary HCL output", removeErr),
		wrapOutputError("sync HCL output directory", syncErr),
		wrapOutputError("close HCL output directory", closeParentErr),
	)
}

func (s *stagedOutput) close() error {
	return wrapOutputError("close HCL output directory", s.closeParent())
}

func (s *stagedOutput) closeParent() error {
	if s.parent == nil {
		return nil
	}
	err := s.parent.Close()
	s.parent = nil
	return err
}

func wrapOutputError(action string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", action, err)
}
