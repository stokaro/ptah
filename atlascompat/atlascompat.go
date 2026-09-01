package atlascompat

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/modelast"
	"go.5x5.cz/ptah/internal/parser"
	"go.5x5.cz/ptah/migration/migrationfile"
)

// PtahSumFileName is the conventional Ptah migration-directory integrity file.
const PtahSumFileName = "ptah.sum"

// AtlasSumFileName is the conventional Atlas migration-directory integrity file.
const AtlasSumFileName = "atlas.sum"

// ParseAtlasHCL parses an Atlas schema HCL document into Ptah's Go schema IR.
//
// A document carrying a top-level env block is an atlas.hcl project file, not a
// schema file, and is refused with an error naming the block and its position.
// Parsing one as a schema would yield an empty IR, which a caller diffing
// against a live database cannot tell apart from "drop everything".
func ParseAtlasHCL(data []byte, filename string) (*schemamodel.Database, error) {
	return atlashcl.Parse(data, filename)
}

// ParseAtlasHCLFile parses an Atlas schema HCL file into Ptah's Go schema IR.
// It refuses project files on the same terms as [ParseAtlasHCL].
func ParseAtlasHCLFile(path string) (*schemamodel.Database, error) {
	return atlashcl.ParseFile(path)
}

// ParseSQLOptions configures ParseSQL.
type ParseSQLOptions struct {
	// Dialect selects dialect-specific parsing behavior. It accepts every
	// spelling core/platform.NormalizeDialect resolves, and a name outside
	// that set selects nothing in particular — pass one Ptah resolves.
	// Empty means compatibility-oriented best effort.
	Dialect string
	// Capabilities selects dialect capabilities for syntax where the same
	// dialect has version-dependent behavior.
	Capabilities capability.Capabilities
	// Timeout caps parser work. Zero keeps Ptah's parser default.
	Timeout time.Duration
}

// ParseSQL parses SQL DDL into Ptah AST statements.
//
// Input holding no DDL — empty text, or only whitespace, comments, and
// semicolons — returns an empty StatementList and a nil error. Statements
// that do not describe schema, such as DML and transaction control, are
// skipped and contribute no node. A syntax error, an unsupported statement,
// and an expired Timeout each return a nil list and an error saying what
// stopped the parse. The package exports no sentinel for these errors: the
// refusal is the contract, and the message is diagnostic text rather than
// something to branch on.
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
//
// The conversion never fails and refuses nothing: each declared object is
// appended as its own statement or carried inline by the statement that
// declares it. Which of the two applies is a deliberate per-platform
// modeling decision rather than an accident — a dialect that has no
// standalone enum type takes the enum on the referencing column instead, so
// an enum nothing references contributes no DDL there, and a dialect that
// cannot add a foreign key once the table exists keeps it inside CREATE
// TABLE. The platform also selects naming and qualification, such as default
// foreign-key constraint names and user-type qualification. Statement order
// is dependency-aware and deterministic: a definition precedes what
// references it, and the same input yields the same sequence. Whether a
// statement can be rendered on a concrete dialect is the renderer's
// capability decision, made downstream where a refusal can be reported.
//
// Canonical platform names are declared in core/platform.
func SchemaToAST(database schemamodel.Database, targetPlatform string) *ast.StatementList {
	return modelast.CollectDatabase(database, targetPlatform)
}

// DBSchemaToGoSchema converts an introspected database schema into Ptah's Go
// schema IR, so a live database can be compared and planned against like an
// authored schema. The conversion is faithful rather than lossy by design:
// what the reader observed — referential actions among them — survives into
// the IR, which is what lets a plan target the introspected database itself
// instead of a reduced copy of it.
//
// dbSchema must be non-nil: there is no error return, so a nil argument is a
// programming error rather than a case this function reports.
func DBSchemaToGoSchema(dbSchema *catalog.Database) *schemamodel.Database {
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

// Bytes renders the sum file in its on-disk form. It returns nil for a nil
// receiver.
func (s *SumFile) Bytes() []byte {
	internal := toInternalSum(s)
	if internal == nil {
		return nil
	}
	return internal.Bytes()
}

// ParseSum parses a Ptah or Atlas h1 migration sum file.
//
// A trailing newline and CRLF line endings are tolerated — a checkout on
// Windows must not report false drift — while structurally malformed content
// (a bad hash, a malformed entry line, a duplicate entry) is an explicit
// error rather than a silent mismatch.
func ParseSum(data []byte) (*SumFile, error) {
	sum, err := migratesum.Parse(data)
	if err != nil {
		return nil, err
	}
	return fromInternalSum(sum), nil
}

// ComputeSum computes a migration-directory sum over fsys.
//
// The sum covers exactly the migration files the selected format recognizes;
// the integrity file itself and non-migration files contribute nothing.
// Entries are sorted by name, so the result is deterministic for a given
// tree. DirFormatPtah computes Ptah's own integrity file. DirFormatAtlas —
// or DirFormatAuto over a directory that carries an atlas.sum — computes the
// Atlas migration-directory integrity format instead, byte for byte, so
// Atlas tooling validates the same directory. An unrecognized format is an
// error.
func ComputeSum(fsys fs.FS, format migrationfile.DirFormat) (*SumFile, error) {
	sum, err := migratesum.ComputeWithFormat(fsys, format)
	if err != nil {
		return nil, err
	}
	return fromInternalSum(sum), nil
}

// SumFileNameForFormat returns the integrity file name for a migration
// directory format: atlas.sum for DirFormatAtlas, ptah.sum for DirFormatPtah
// and DirFormatAuto. An unrecognized format is an error.
func SumFileNameForFormat(format migrationfile.DirFormat) (string, error) {
	return migratesum.FileNameForFormat(format)
}

// WriteSum computes and writes a migration-directory sum file. The sum is
// computed as by [ComputeSum], written atomically to the integrity file
// [SumFileNameForFormat] names for format, and returned.
func WriteSum(dir string, format migrationfile.DirFormat) (*SumFile, error) {
	sum, err := migratesum.WriteWithFormat(dir, format)
	if err != nil {
		return nil, err
	}
	return fromInternalSum(sum), nil
}

// VerifySum verifies a migration-directory sum over fsys.
//
// Drift is reported in the SumResult rather than as an error, so callers
// choose the exit code. An error is reserved for a directory that cannot be
// verified at all: a missing integrity file, and one that cannot be parsed.
// The package exports no sentinel for either, so the refusal is the contract
// and the message is diagnostic text rather than something to branch on.
//
// An explicit format checks only its own integrity file: ptah.sum for
// DirFormatPtah, atlas.sum for DirFormatAtlas. DirFormatAuto selects
// atlas.sum when it is the only integrity file present, ptah.sum otherwise,
// and refuses a directory carrying both. SumResult.SumFileName reports which
// file was compared.
func VerifySum(fsys fs.FS, format migrationfile.DirFormat) (*SumResult, error) {
	result, err := migratesum.VerifyWithFormat(fsys, format)
	if err != nil {
		return nil, err
	}
	return fromInternalResult(result), nil
}

// VerifySumDir verifies a migration-directory sum on disk. It is [VerifySum]
// over the directory at dir, with the same error and drift contracts.
func VerifySumDir(dir string, format migrationfile.DirFormat) (*SumResult, error) {
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

// OK reports whether the directory matches its recorded sum exactly. A nil
// result reports false.
func (r *SumResult) OK() bool {
	return r != nil &&
		len(r.Added) == 0 &&
		len(r.Removed) == 0 &&
		len(r.Changed) == 0 &&
		!r.DirHashMismatch
}

// Describe renders a drift result as human-readable lines. It returns the
// empty string when the result is nil or OK.
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
	return os.WriteFile(filepath.Join(dir, name), sum.Bytes(), 0644) // #nosec G306 -- Sum files are committed project files.
}
