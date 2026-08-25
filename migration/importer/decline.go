package importer

import (
	"fmt"
	"io/fs"
	"path"
	"slices"
	"sort"
	"strings"
)

// DeclinedFile is one file found under the source directory that the import did
// not convert into a Ptah migration, together with the reason.
//
// Every file in the source tree is either converted or declined. The importer
// used to drop whatever its parser did not recognize with
// `continue // ignore non-migration files`, so a migration one directory down,
// a Flyway baseline, or a name off by one character left no trace in the output
// and no trace in `ptah.sum` -- which was then written over the surviving
// subset, so the result validated clean and nothing downstream could establish
// that SQL had been lost (stokaro/ptah#2231).
type DeclinedFile struct {
	// Path is the file's slash-separated path relative to the source root.
	Path string
	// Reason says why the file was not converted, in a sentence a user can act
	// on. It names the rule rather than restating that the file was skipped.
	Reason string
	// CarriesSQL reports whether this file could hold migration SQL, which is
	// what makes declining it a possible loss rather than a note.
	//
	// The distinction is what keeps the refusal usable: a migrations directory
	// almost always holds a README, and refusing every import that saw one
	// would make the guard something users route around rather than read. A
	// declined `.sql` file is the case the guard exists for -- it is the
	// material this command converts, and the one whose loss ptah.sum would
	// then certify as clean.
	CarriesSQL bool
}

// ParseResult is what a Parser read from one source directory.
//
// Consumed and Declined together are how the importer proves it looked at
// everything: [AccountForSource] walks the source tree and any file in neither
// list is reported as declined for an unrecognized name. A parser that forgets
// to record a file it used therefore over-reports rather than under-reports,
// which is the safe direction -- the failure is visible in the output instead of
// being a migration that quietly vanished.
type ParseResult struct {
	// Migrations are the migrations read, in the parser's own order.
	Migrations []SourceMigration
	// Consumed are the source paths that became those migrations.
	Consumed []string
	// Declined are the paths the parser declined for a reason it knows, such as
	// a Flyway baseline or a file below a top level the tool does not read.
	Declined []DeclinedFile
}

// consume records that path became part of a migration.
func (r *ParseResult) consume(file string) {
	r.Consumed = append(r.Consumed, file)
}

// decline records that path was found and not converted, and why.
func (r *ParseResult) decline(file, reason string) {
	r.Declined = append(r.Declined, DeclinedFile{
		Path: file, Reason: reason, CarriesSQL: carriesSQL(file),
	})
}

// carriesSQL reports whether a source path could hold migration SQL.
//
// The extension is matched case-insensitively on purpose: `.up.SQL` was one of
// the names the importer dropped without a word, and a file whose only defect is
// a shouted extension is exactly the migration a user would expect to be told
// about (stokaro/ptah#2231).
func carriesSQL(name string) bool {
	return strings.EqualFold(path.Ext(name), ".sql")
}

// BlockingDeclines returns the declined files whose loss ptah.sum must not
// certify as a clean directory.
func BlockingDeclines(declined []DeclinedFile) []DeclinedFile {
	blocking := make([]DeclinedFile, 0, len(declined))
	for _, entry := range declined {
		if entry.CarriesSQL {
			blocking = append(blocking, entry)
		}
	}
	return blocking
}

// AccountForSource completes a parser's result so every file under the source
// tree is accounted for, and returns the declined set in path order.
//
// The walk is the authority, not the parser: a parser reports what it used and
// what it deliberately turned down, and everything else in the tree is declined
// here for an unrecognized name. That is what makes the accounting total rather
// than a per-parser courtesy -- each of the five parsers had the same three
// lines that dropped unmatched names, so this cannot be left to them.
func AccountForSource(fsys fs.FS, parser Parser, result *ParseResult) ([]DeclinedFile, error) {
	present, err := sourceFiles(fsys)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool, len(result.Consumed)+len(result.Declined))
	for _, used := range result.Consumed {
		seen[used] = true
	}
	declined := make([]DeclinedFile, 0, len(result.Declined))
	for _, entry := range result.Declined {
		if seen[entry.Path] {
			continue
		}
		seen[entry.Path] = true
		declined = append(declined, entry)
	}

	for _, file := range present {
		if seen[file] {
			continue
		}
		declined = append(declined, DeclinedFile{
			Path: file, Reason: unrecognizedNameReason(parser), CarriesSQL: carriesSQL(file),
		})
	}

	sort.Slice(declined, func(i, j int) bool { return declined[i].Path < declined[j].Path })
	return declined, nil
}

// unrecognizedNameReason names the rule a file failed rather than saying it was
// skipped, because the importer cannot tell a README from an author's migration
// that missed the naming rule by one character -- and the user can.
func unrecognizedNameReason(parser Parser) string {
	return fmt.Sprintf("its name is not a %s migration file name (%s)",
		parser.Name(), parser.NamePattern())
}

// subdirectoryReason names why a file below the top level was not read, for the
// tools that read only the top level.
//
// golang-migrate itself does not read subfolders, so declining them stays
// correct; announcing it is the part that was missing.
func subdirectoryReason(tool string) string {
	return fmt.Sprintf("it sits below the top level, and %s reads only the top level of the source directory", tool)
}

// sourceFiles lists every regular file under fsys, slash-separated and relative
// to the root, excluding the dot-directories a checkout carries.
func sourceFiles(fsys fs.FS) ([]string, error) {
	var files []string
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// A dot-directory is version-control or editor bookkeeping, not
			// migrations a user is waiting to see converted. Reporting `.git`
			// file by file would bury the one line that matters.
			if name != "." && strings.HasPrefix(path.Base(name), ".") {
				return fs.SkipDir
			}
			return nil
		}
		files = append(files, name)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read source directory: %w", err)
	}
	slices.Sort(files)
	return files, nil
}

// topLevelOnly returns the entries a flat parser reads, and records every file
// below the top level as declined.
//
// The four tools whose own readers are flat share this: they see the same set of
// files and turn down the same ones for the same reason, so the rule is written
// once.
func topLevelOnly(fsys fs.FS, tool string, result *ParseResult) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read source directory: %w", err)
	}
	nested, err := sourceFiles(fsys)
	if err != nil {
		return nil, err
	}
	for _, file := range nested {
		if strings.Contains(file, "/") {
			result.decline(file, subdirectoryReason(tool))
		}
	}
	return entries, nil
}

// PartialImportError refuses an import that would write ptah.sum over a subset
// of the source directory.
//
// It carries the declined set rather than a formatted string so the CLI can
// present it the same way it presents a successful import's declined list, and
// so a caller can tell a partial import from any other failure.
type PartialImportError struct {
	Declined []DeclinedFile
}

func (e *PartialImportError) Error() string {
	names := make([]string, 0, len(e.Declined))
	for _, entry := range e.Declined {
		names = append(names, entry.Path)
	}
	return fmt.Sprintf(
		"refusing to write ptah.sum for a partial import: %d source file(s) were not converted (%s); "+
			"pass --allow-partial to import the rest anyway",
		len(e.Declined), strings.Join(names, ", "))
}

// nestedDetection is a source tool recognized only below the top level.
type nestedDetection struct {
	parser   Parser
	examples []string
}

// detectBelowTopLevel reports a tool whose migrations sit under a subdirectory
// rather than at the source root, or nil when none does.
//
// It exists to turn "could not detect the source migration tool" into a
// statement about the layout. The directory in the report was unambiguously
// golang-migrate; the message sent the user to --from, and --from then failed
// with "no golang-migrate migration files found" -- two messages, neither
// naming the depth (stokaro/ptah#2231).
//
// Flyway is not consulted: it reads the tree recursively, so a Flyway directory
// laid out this way is detected normally and never reaches here.
func detectBelowTopLevel(fsys fs.FS) *nestedDetection {
	files, err := sourceFiles(fsys)
	if err != nil {
		return nil
	}
	dirs := make(map[string]bool)
	for _, file := range files {
		if dir := path.Dir(file); dir != "." {
			dirs[dir] = true
		}
	}
	if len(dirs) == 0 {
		return nil
	}
	ordered := make([]string, 0, len(dirs))
	for dir := range dirs {
		ordered = append(ordered, dir)
	}
	slices.Sort(ordered)

	for _, parser := range Parsers() {
		for _, dir := range ordered {
			sub, err := fs.Sub(fsys, dir)
			if err != nil || !parser.Detect(sub) {
				continue
			}
			examples := make([]string, 0, 2)
			for _, file := range files {
				if path.Dir(file) == dir {
					examples = append(examples, file)
				}
				if len(examples) == 2 {
					break
				}
			}
			return &nestedDetection{parser: parser, examples: examples}
		}
	}
	return nil
}
