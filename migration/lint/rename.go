package lint

// A rename retires one logical name and introduces another. That is a single
// event, and the two command surfaces describe it at different altitudes:
//
//   - Native `ptah migrations lint` describes the consequence. BC101 says
//     deployed application versions that still reference the old name fail
//     instantly, and prescribes add-new/backfill/drop-old across releases.
//   - `ptah-compat migrate lint` describes the structure, because that is what
//     the analyzer it is compatible with reports: the old name no longer
//     resolves, which is a destructive change to that name (DS101 for a table,
//     DS102 for a column in Ptah's code space).
//
// They are two descriptions of one event, not two events, so exactly one of
// them is emitted per rename and [File.compatibility] selects which. Emitting
// both would report one statement twice, and on the compatibility surface would
// print two diagnostics where the analyzer it matches prints one.
//
// The destructive classification lives in the DS family rather than under a BC
// code because suppression selectors are keyed by family. Measured against the
// pinned community binary: `-- atlas:nolint destructive` above a rename
// silences it (exit 0), while `-- atlas:nolint incompatible` above the same
// rename does not (still DS103, exit 1). `destructive` maps to the DS and CD
// families and `incompatible` to BC, so a rename carrying a BC code on the
// compatibility surface would be silenced by the selector that must not silence
// it -- widening what a suppression covers, which is the one direction a
// compatibility surface may never move in.

// renamedName is one logical name a statement retires by renaming it.
//
// Name is empty when the statement is recognizably a rename but the retired
// name could not be read from it. That case must still report: an unreadable
// name is not evidence that nothing was renamed, so the owning rule emits a
// subject-less finding rather than staying silent.
type renamedName struct {
	kind SubjectKind
	// name is the retired name exactly as the source spells it.
	name string
	// normalized is the retired name in the linter's comparison form, used to
	// look the retired object up in the schema state the version starts from.
	normalized string
	// introduced is the name the rename puts in place of the retired one,
	// source-spelled. Empty when the statement is a rename whose target could
	// not be read.
	introduced string
	// parent is the owning table of a renamed column, source-spelled.
	parent string
	// owner is the normalized reference of the table whose creation earlier in
	// the same file exempts this rename. For a column rename that is the table
	// the column belongs to; for a table rename, the table itself.
	owner string
}

// renamedNames returns the logical names a statement retires by renaming them.
//
// It recognizes every rename form the linter's scanner can reach:
//
//	ALTER TABLE t RENAME TO u          -- table (RENAME AS u is the synonym)
//	ALTER TABLE t RENAME u             -- table, MySQL's bare form
//	ALTER TABLE t RENAME COLUMN a TO b -- column
//	ALTER TABLE t RENAME a TO b        -- column, COLUMN keyword omitted
//	RENAME TABLE t TO u, v TO w        -- one table per pair
//
// Index, key and constraint renames are deliberately absent: they are invisible
// to deployed application code, and the analyzer this tool is compatible with
// reports nothing for them either (measured on `ALTER INDEX ... RENAME TO` and
// `ALTER TABLE ... RENAME CONSTRAINT`, both silent on both sides).
func renamedNames(w, sourceWords []string) []renamedName {
	if hasWordPrefix(w, "RENAME", "TABLE") {
		return renamedTableList(w, sourceWords)
	}
	if !isAlterTable(w) {
		return nil
	}
	return alterRenamedNames(w, sourceWords)
}

// renamedTableList reads the pair list of a standalone RENAME TABLE statement.
// A pair that cannot be read yields one unnamed table rename and ends the scan,
// so an unreadable tail still reports instead of silently shortening the list.
func renamedTableList(w, sourceWords []string) []renamedName {
	var names []renamedName
	j := 2
	for {
		source, next := tableRefAt(w, sourceWords, j)
		if source.normalized == "" || next >= len(w) || (w[next] != "TO" && w[next] != "AS") {
			return append(names, renamedName{kind: SubjectTable})
		}
		target, afterTarget := tableRefAt(w, sourceWords, next+1)
		if target.normalized == "" {
			return append(names, renamedName{kind: SubjectTable})
		}
		names = append(names, renamedName{
			kind:  SubjectTable,
			name:  source.name,
			owner: source.normalized,
		})
		if afterTarget < len(w) && w[afterTarget] == "," {
			j = afterTarget + 1
			continue
		}
		return names
	}
}

// alterRenamedNames reads the RENAME clauses of an ALTER TABLE statement.
func alterRenamedNames(w, sourceWords []string) []renamedName {
	table := alterTableReference(w, sourceWords)
	renamedTable := renamedName{kind: SubjectTable, name: table.name, owner: table.normalized}
	var names []renamedName
	for _, i := range clauseStarts(w) {
		if i >= len(w) || w[i] != "RENAME" || i+1 >= len(w) {
			continue
		}
		j := i + 1
		explicitColumn := false
		switch w[j] {
		case "INDEX", "KEY", "CONSTRAINT":
			continue
		case "TO", "AS":
			names = append(names, renamedTable)
			continue
		case "COLUMN":
			explicitColumn = true
			j++
		}
		identifier, next, ok := identifierAt(w, sourceWords, j)
		if !ok {
			names = append(names, renamedName{kind: SubjectColumn})
			continue
		}
		if next < len(w) && (w[next] == "TO" || w[next] == "AS") {
			introduced, _, _ := identifierAt(w, sourceWords, next+1)
			names = append(names, renamedName{
				kind:       SubjectColumn,
				name:       identifier.name,
				normalized: identifier.normalized,
				introduced: introduced.name,
				parent:     table.name,
				owner:      table.normalized,
			})
			continue
		}
		if explicitColumn {
			// RENAME COLUMN a, with no target: recognizably a column rename
			// whose retired name cannot be trusted.
			names = append(names, renamedName{kind: SubjectColumn})
			continue
		}
		// MySQL's `ALTER TABLE t RENAME new_name`: the identifier is the new
		// table name, so the retired name is the table's own.
		names = append(names, renamedTable)
	}
	return names
}

// statementRename is the rename content of one statement after the exemption
// below has been applied.
type statementRename struct {
	statementIndex int
	line           int
	names          []renamedName
}

// fileRenames walks an up migration and returns the renames each statement
// performs, minus those whose object this same file created.
//
// The exemption mirrors the create-then-drop exemption DS101 already applies,
// and holds for the same reason: no deployed application version ever saw a
// name this migration itself introduced, so retiring it breaks nothing. It is
// measured on both forms -- `CREATE TABLE users (id int); ALTER TABLE users
// RENAME COLUMN id TO oid;` and `CREATE TABLE staging (id int); ALTER TABLE
// staging RENAME TO users;` in one file are both silent on the analyzer this
// tool is compatible with -- and it applies on the native surface too, where
// BC101 was reporting a hazard that cannot occur.
func fileRenames(file *File) []statementRename {
	if !file.IsUp {
		return nil
	}
	var renames []statementRename
	created := map[string]bool{}
	for i := range file.Statements {
		stmt := &file.Statements[i]
		if ref := createdTableRef(stmt.Words); ref != "" {
			created[ref] = true
			continue
		}
		var kept []renamedName
		for _, name := range renamedNames(stmt.Words, stmt.sourceWords) {
			if refersToCreated(created, name.owner) {
				continue
			}
			kept = append(kept, name)
		}
		if len(kept) == 0 {
			continue
		}
		renames = append(renames, statementRename{
			statementIndex: i,
			line:           stmt.Line,
			names:          kept,
		})
	}
	return renames
}

// renameAddSideCandidates returns the column renames whose add side this file's
// surface reports, before any schema state has been consulted.
//
// Both the request for that state ([baselineVersions]) and the finding built
// from it ([renamedColumnAddFindings]) go through this one function, so the
// surface split is decided in exactly one place. Deciding it twice made the
// native surface's control unable to see the rule's own gate disappear: with no
// baseline ever requested there, the finding could not fire whether that gate
// was present or not, and the control stayed green either way.
func renameAddSideCandidates(file *File) []statementRename {
	if file.compatibility != CompatibilityProfileAtlas {
		return nil
	}
	return renamesOfKind(fileRenames(file), SubjectColumn)
}

// renamesOfKind keeps the renames of one object kind, dropping statements left
// with none.
func renamesOfKind(renames []statementRename, kind SubjectKind) []statementRename {
	var out []statementRename
	for _, rename := range renames {
		var kept []renamedName
		for _, name := range rename.names {
			if name.kind == kind {
				kept = append(kept, name)
			}
		}
		if len(kept) == 0 {
			continue
		}
		out = append(out, statementRename{
			statementIndex: rename.statementIndex,
			line:           rename.line,
			names:          kept,
		})
	}
	return out
}
