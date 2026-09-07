package atlas

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/cli/internal/webartifact"
	"ptah.run/internal/fileopen"
	"ptah.run/internal/schemadoc"
	"ptah.run/migration/schemadiff/difftypes"
)

// `--web` and what it does here.
//
// Atlas documents the flag as "open the schema ERD in the browser" on its full
// distribution. The pinned community binary registers no such flag -- measured,
// `unknown flag: --web`, with `--format` present as a control -- so there is no
// community behavior to match and none to be looser or stricter than. The
// semantics below are Ptah's own, and two of them are deliberate divergences
// from the vendor description worth stating rather than discovering:
//
//   - Nothing is published and nothing is fetched. A plausible reading of
//     "open the ERD in the browser" is a hosted URL; a schema is the shape of
//     somebody's data, and a flag that uploads it is a flag that leaks it. The
//     artifact is a self-contained file on this machine, and the browser opens
//     that file.
//   - The artifact is written whether or not a browser opens. Opening is a
//     desktop action with environments where it cannot happen, and a run that
//     failed because a machine has no display would fail for a reason that has
//     nothing to do with the schema.
//
// Under `PTAH_ATLAS_STRICT_COMPAT=1` the flag is not registered at all, because
// the pinned binary does not register it. The registration below sits inside
// the same `if !policy.IsStrictCE()` block every non-community flag does.
//
// The suppression is [fileopen.SkipEnvVar] rather than a flag. A flag the
// pinned binary does not have would break the conformance cli-surface tier,
// which asserts that `ptah-compat` registers exactly the flags that binary
// registers -- the same reason `PTAH_SKIP_CHECKS` is a variable.

// atlasWebRequest reads the `--web` flag and the suppression that goes with it.
//
// Both are resolved together and before any work, so an invocation that names
// neither still refuses a malformed `PTAH_SKIP_BROWSER_OPEN`. Resolving it
// only when `--web` was passed would leave a typo dormant until the day someone
// asks for the diagram.
func atlasWebRequest(cmd *cobra.Command) (web, skipOpen bool, err error) {
	skipOpen, err = fileopen.SkipRequested()
	if err != nil {
		return false, false, err
	}
	// The VALUE, not just Changed: cobra marks a boolean changed for
	// `--web=false` too, and a generated command line passes explicit booleans.
	if cmd.Flags().Lookup(atlasSchemaWebFlagName) == nil {
		return false, skipOpen, nil
	}
	web, err = cmd.Flags().GetBool(atlasSchemaWebFlagName)
	if err != nil {
		return false, false, err
	}
	return web, skipOpen, nil
}

// atlasSchemaERD is one ERD artifact request.
type atlasSchemaERD struct {
	schema   *schemamodel.Database
	title    string
	source   string
	changes  map[string]schemadoc.ChangeKind
	skipOpen bool
}

// writeAtlasSchemaERD writes the artifact and says where it went.
//
// The report goes to the diagnostics stream. On `schema inspect` standard
// output is the inspected schema and a pipeline reads it, so a path printed
// there would corrupt the very output the flag is meant to sit beside.
func writeAtlasSchemaERD(cmd *cobra.Command, request atlasSchemaERD) error {
	if request.schema == nil {
		return fmt.Errorf("--%s: this source produced no schema to draw", atlasSchemaWebFlagName)
	}
	result, diagnostics, err := webartifact.Write(cmd.Context(), request.schema, webartifact.Options{
		Title:   request.title,
		Source:  request.source,
		Changes: request.changes,
		Skip:    request.skipOpen,
	})
	if err != nil {
		return fmt.Errorf("--%s: %w", atlasSchemaWebFlagName, err)
	}
	for _, diagnostic := range diagnostics {
		fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s\n", diagnostic)
	}
	// Both sentences: this command's standard output is the schema or the
	// migration SQL, so nothing else has told the operator a file exists.
	webartifact.ReportArtifact(cmd.ErrOrStderr(), result.Path)
	webartifact.ReportOpen(cmd.ErrOrStderr(), result)
	return nil
}

// atlasSchemaERDSource names the source under the document title.
//
// A URL is reduced to its scheme and host. The document is meant to be shared
// by copying, and a data source URL carries credentials: writing it into a file
// somebody attaches to a review is how a password reaches a pull request.
func atlasSchemaERDSource(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" {
		// Not a URL Ptah can take apart. Naming the scheme it might have had
		// would be a guess, and echoing the raw value is what this function
		// exists to avoid.
		return "a schema source"
	}
	if parsed.Host == "" {
		return parsed.Scheme
	}
	return parsed.Scheme + "://" + parsed.Hostname()
}

// atlasDiffERD builds the schema the diff diagram draws, and the marks on it.
//
// The diagram draws the END state plus the tables that leave it. A removed
// table is in neither `--to` nor the document that would be rendered from it,
// so drawing only the end state would answer "what will exist" where the flag
// was passed to a command whose subject is what changes. The union is built
// here, in the surface that knows both states, rather than in the renderer:
// schemadoc draws one schema and takes the marks as data, so a comparison it
// cannot see is a comparison it cannot get wrong.
//
// A table that changed is marked once, whatever changed inside it. The diagram
// draws tables, and a column-level mark has no node to sit on; the table card
// below the diagram already lists the columns.
func atlasDiffERD(
	report atlasreport.SchemaDiff,
	changes *difftypes.SchemaDiff,
) (*schemamodel.Database, map[string]schemadoc.ChangeKind) {
	marks := make(map[string]schemadoc.ChangeKind)
	if changes != nil {
		for _, name := range changes.TablesAdded.Names() {
			marks[name] = schemadoc.ChangeAdded
		}
		for _, table := range changes.TablesModified {
			marks[table.TableName] = schemadoc.ChangeChanged
		}
		for _, name := range changes.TablesRemoved {
			marks[name] = schemadoc.ChangeRemoved
		}
	}
	drawn := unionWithRemovedTables(report.To, report.From, removedTableNames(changes))
	return drawn, marks
}

// removedTableNames is the tables the comparison says leave.
func removedTableNames(changes *difftypes.SchemaDiff) []string {
	if changes == nil {
		return nil
	}
	return changes.TablesRemoved
}

// unionWithRemovedTables copies the end state and adds back the tables that
// leave it, so a removed table has a node to be marked on.
//
// Only the parts the diagram and the table cards read are carried over: the
// table, its fields and its indexes. A removed table's enums, policies and
// triggers are not drawn and would only widen the document with objects the
// end state does not have.
//
// A name already present in the end state is skipped. A comparison should not
// report a table as both removed and present, and if one ever did, the end
// state is the answer this document is about.
func unionWithRemovedTables(to, from *schemamodel.Database, removed []string) *schemamodel.Database {
	if to == nil || from == nil || len(removed) == 0 {
		return to
	}
	present := make(map[string]bool, len(to.Tables))
	structs := make(map[string]bool, len(to.Tables))
	for _, table := range to.Tables {
		present[table.Name] = true
		structs[table.StructName] = true
	}

	// A struct copy shares its slices, so appending to union.Tables could write
	// into the array `to` still points at whenever it has spare capacity -- the
	// caller's end state would grow a table it does not have, invisibly and only
	// sometimes. The three slices this function appends to are cloned; the rest
	// are read and never written.
	union := *to
	union.Tables = slices.Clone(to.Tables)
	union.Fields = slices.Clone(to.Fields)
	union.Indexes = slices.Clone(to.Indexes)
	for _, name := range removed {
		for _, table := range from.Tables {
			if table.Name != name || present[name] || structs[table.StructName] {
				continue
			}
			union.Tables = append(union.Tables, table)
			for _, field := range from.Fields {
				if field.StructName == table.StructName {
					union.Fields = append(union.Fields, field)
				}
			}
			for _, index := range from.Indexes {
				if index.StructName == table.StructName {
					union.Indexes = append(union.Indexes, index)
				}
			}
		}
	}
	return &union
}

// firstNonEmpty is the first value worth naming, for a flag that takes several.
func firstNonEmpty(values []string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
