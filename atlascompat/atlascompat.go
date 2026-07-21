package atlascompat

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/capability"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlashcl"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
	"github.com/stokaro/ptah/internal/convert/fromschema"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/parser"
	"github.com/stokaro/ptah/migration/migrator"
)

// PtahSumFileName is the conventional Ptah migration-directory integrity file.
const PtahSumFileName = "ptah.sum"

// AtlasSumFileName is the conventional Atlas migration-directory integrity file.
const AtlasSumFileName = "atlas.sum"

// ParseAtlasHCL parses an Atlas schema HCL document into Ptah's Go schema IR.
func ParseAtlasHCL(data []byte, filename string) (*goschema.Database, error) {
	return atlashcl.Parse(data, filename)
}

// ParseAtlasHCLFile parses an Atlas schema HCL file into Ptah's Go schema IR.
func ParseAtlasHCLFile(path string) (*goschema.Database, error) {
	return atlashcl.ParseFile(path)
}

// ParseSQLOptions configures ParseSQL.
type ParseSQLOptions struct {
	// Dialect selects dialect-specific parsing behavior. Empty means
	// compatibility-oriented best effort.
	Dialect string
	// Capabilities selects dialect capabilities for syntax where the same
	// dialect has version-dependent behavior.
	Capabilities capability.Capabilities
	// Timeout caps parser work. Zero keeps Ptah's parser default.
	Timeout time.Duration
}

// ParseSQL parses SQL DDL into Ptah AST statements.
func ParseSQL(sql string, opts ParseSQLOptions) (*ast.StatementList, error) {
	parserOpts := make([]parser.Option, 0, 3)
	if strings.TrimSpace(opts.Dialect) != "" {
		parserOpts = append(parserOpts, parser.WithDialect(opts.Dialect))
	}
	if len(opts.Capabilities) > 0 {
		parserOpts = append(parserOpts, parser.WithCapabilities(opts.Capabilities))
	}
	if opts.Timeout > 0 {
		parserOpts = append(parserOpts, parser.WithTimeout(opts.Timeout))
	}
	return parser.NewParser(sql, parserOpts...).Parse()
}

// SchemaToAST converts Ptah's Go schema IR into SQL AST statements for the
// selected target platform.
func SchemaToAST(database goschema.Database, targetPlatform string) *ast.StatementList {
	return fromschema.FromDatabase(database, targetPlatform)
}

// DBSchemaToGoSchema converts an introspected database schema into Ptah's Go
// schema IR.
func DBSchemaToGoSchema(dbSchema *dbschematypes.DBSchema) *goschema.Database {
	return dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)
}

// SumEntry is one migration file and its content hash.
type SumEntry struct {
	// Name is the slash-separated path of the file relative to the migrations
	// directory.
	Name string
	// Hash is the h1: content hash of the file.
	Hash string
}

// SumFile is a migration-directory integrity file.
type SumFile struct {
	// DirHash is the directory-level hash.
	DirHash string
	// Entries are per-file hashes sorted by name.
	Entries []SumEntry
}

// Bytes renders the sum file in its on-disk form.
func (s *SumFile) Bytes() []byte {
	internal := toInternalSum(s)
	if internal == nil {
		return nil
	}
	return internal.Bytes()
}

// ParseSum parses a Ptah or Atlas h1 migration sum file.
func ParseSum(data []byte) (*SumFile, error) {
	sum, err := migratesum.Parse(data)
	if err != nil {
		return nil, err
	}
	return fromInternalSum(sum), nil
}

// ComputeSum computes a migration-directory sum over fsys.
func ComputeSum(fsys fs.FS, format migrator.MigrationDirFormat) (*SumFile, error) {
	sum, err := migratesum.ComputeWithFormat(fsys, format)
	if err != nil {
		return nil, err
	}
	return fromInternalSum(sum), nil
}

// SumFileNameForFormat returns the integrity file name for a migration
// directory format.
func SumFileNameForFormat(format migrator.MigrationDirFormat) (string, error) {
	return migratesum.FileNameForFormat(format)
}

// WriteSum computes and writes a migration-directory sum file.
func WriteSum(dir string, format migrator.MigrationDirFormat) (*SumFile, error) {
	sum, err := migratesum.WriteWithFormat(dir, format)
	if err != nil {
		return nil, err
	}
	return fromInternalSum(sum), nil
}

// VerifySum verifies a migration-directory sum over fsys.
func VerifySum(fsys fs.FS, format migrator.MigrationDirFormat) (*SumResult, error) {
	result, err := migratesum.VerifyWithFormat(fsys, format)
	if err != nil {
		return nil, err
	}
	return fromInternalResult(result), nil
}

// VerifySumDir verifies a migration-directory sum on disk.
func VerifySumDir(dir string, format migrator.MigrationDirFormat) (*SumResult, error) {
	result, err := migratesum.VerifyDirWithFormat(dir, format)
	if err != nil {
		return nil, err
	}
	return fromInternalResult(result), nil
}

// SumResult is the outcome of a migration-directory integrity verification.
type SumResult struct {
	// Added are migration files present on disk but absent from the sum file.
	Added []string
	// Removed are files recorded in the sum file but missing on disk.
	Removed []string
	// Changed are files whose content hash no longer matches the sum file.
	Changed []string
	// DirHashMismatch is set when the directory hash differs while per-file
	// entries match.
	DirHashMismatch bool
	// SumFileName is the integrity file this result was compared against.
	SumFileName string
}

// OK reports whether the directory matches its recorded sum exactly.
func (r *SumResult) OK() bool {
	return r != nil &&
		len(r.Added) == 0 &&
		len(r.Removed) == 0 &&
		len(r.Changed) == 0 &&
		!r.DirHashMismatch
}

// Describe renders a drift result as human-readable lines.
func (r *SumResult) Describe() string {
	if r == nil || r.OK() {
		return ""
	}
	name := r.SumFileName
	if name == "" {
		name = PtahSumFileName
	}
	lines := []string{"migration directory does not match " + name + ":"}
	for _, changed := range r.Changed {
		lines = append(lines, "  changed: "+changed)
	}
	for _, added := range r.Added {
		lines = append(lines, "  added (not in "+name+"): "+added)
	}
	for _, removed := range r.Removed {
		lines = append(lines, "  removed (still in "+name+"): "+removed)
	}
	if r.DirHashMismatch {
		lines = append(lines, "  directory hash mismatch ("+name+" was hand-edited)")
	}
	return strings.Join(lines, "\n")
}

func fromInternalSum(sum *migratesum.SumFile) *SumFile {
	if sum == nil {
		return nil
	}
	entries := make([]SumEntry, len(sum.Entries))
	for i, entry := range sum.Entries {
		entries[i] = SumEntry{Name: entry.Name, Hash: entry.Hash}
	}
	return &SumFile{DirHash: sum.DirHash, Entries: entries}
}

func toInternalSum(sum *SumFile) *migratesum.SumFile {
	if sum == nil {
		return nil
	}
	entries := make([]migratesum.Entry, len(sum.Entries))
	for i, entry := range sum.Entries {
		entries[i] = migratesum.Entry{Name: entry.Name, Hash: entry.Hash}
	}
	return &migratesum.SumFile{DirHash: sum.DirHash, Entries: entries}
}

func fromInternalResult(result *migratesum.Result) *SumResult {
	if result == nil {
		return nil
	}
	return &SumResult{
		Added:           append([]string(nil), result.Added...),
		Removed:         append([]string(nil), result.Removed...),
		Changed:         append([]string(nil), result.Changed...),
		DirHashMismatch: result.DirHashMismatch,
		SumFileName:     result.SumFileName,
	}
}

// WriteSumBytes writes sum to dir/name. It is useful for tools that need to
// materialize a sum produced elsewhere while keeping Ptah's on-disk format.
func WriteSumBytes(dir, name string, sum *SumFile) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("sum file name is required")
	}
	if sum == nil {
		return fmt.Errorf("sum file is required")
	}
	return os.WriteFile(filepath.Join(dir, name), sum.Bytes(), 0644) //nolint:gosec // Sum files are committed project files.
}
