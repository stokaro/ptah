package lint

import (
	"slices"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// readersAtColumnDrop removes a baseline view only when earlier statements in
// this file dropped it or rebuilt it with a body that cannot name the column.
// Other readers retain their baseline evidence.
func readersAtColumnDrop(file *File, index int, column string, readers []lintext.BaselineDependent) []lintext.BaselineDependent {
	return slices.DeleteFunc(slices.Clone(readers), func(reader lintext.BaselineDependent) bool {
		return reader.Kind == "view" && viewReleasedBefore(file.Statements[:index], reader.Dependent, column)
	})
}

func viewReleasedBefore(statements []Statement, name, column string) bool {
	name = lintkit.NormalizeIdent(name)
	released := false
	for i := range statements {
		statement := &statements[i]
		if droppedView(statement, name) {
			released = true
			continue
		}
		created, body := createdView(statement)
		if created == name {
			// A replacement is trusted only after the file removed the baseline
			// view. Otherwise the original dependency may still stand.
			released = released && viewBodyExcludesColumn(body, column)
		}
	}
	return released
}

func droppedView(statement *Statement, name string) bool {
	w := statement.Words
	if !lintkit.HasWordPrefix(w, "DROP", "VIEW") {
		return false
	}
	for at := lintkit.SkipIfExists(w, 2); at < len(w); {
		ref, next := lintkit.TableRefAt(w, statement.sourceWords, at)
		if ref.Normalized == "" {
			return false
		}
		if ref.Normalized == name {
			return true
		}
		if next >= len(w) || w[next] != "," {
			return false
		}
		at = next + 1
	}
	return false
}

func createdView(statement *Statement) (name string, body []string) {
	w := statement.Words
	if len(w) == 0 || w[0] != "CREATE" {
		return "", nil
	}
	at := 1
	if lintkit.HasWordPrefix(w[at:], "OR", "REPLACE") {
		at += 2
	}
	if at < len(w) && (w[at] == "TEMP" || w[at] == "TEMPORARY") {
		at++
	}
	if at >= len(w) || w[at] != "VIEW" {
		return "", nil
	}
	at++
	if lintkit.HasWordPrefix(w[at:], "IF", "NOT", "EXISTS") {
		at += 3
	}
	ref, next := lintkit.TableRefAt(w, statement.sourceWords, at)
	if ref.Normalized == "" {
		return "", nil
	}
	for at = next; at < len(w); at++ {
		if w[at] == "AS" {
			return ref.Normalized, w[at+1:]
		}
	}
	return ref.Normalized, nil
}

// viewBodyExcludesColumn proves absence only for a plain projection from one
// table. An expression, alias, wildcard, join or clause may read a column
// without naming it in the select list, so the baseline warning remains.
func viewBodyExcludesColumn(body []string, column string) bool {
	if len(body) < 4 || body[0] != "SELECT" {
		return false
	}
	from := slices.Index(body, "FROM")
	if from < 2 {
		return false
	}
	table, next := lintkit.TableRefAt(body, body, from+1)
	if table.Normalized == "" || (next < len(body) && (next+1 != len(body) || body[next] != ";")) {
		return false
	}
	column = lintkit.NormalizeIdent(column)
	for at := 1; at < from; {
		selected, end := lintkit.TableRefAt(body[:from], body[:from], at)
		if selected.Normalized == "" {
			return false
		}
		member := selected.Normalized[strings.LastIndex(selected.Normalized, ".")+1:]
		if member == column || selected.Normalized == table.Normalized ||
			member == table.Normalized[strings.LastIndex(table.Normalized, ".")+1:] {
			return false
		}
		at = end
		if at < from {
			if body[at] != "," {
				return false
			}
			at++
		}
	}
	return true
}
