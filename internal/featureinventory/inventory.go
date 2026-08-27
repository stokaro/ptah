package featureinventory

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// InventoryPath is where the feature inventory lives, relative to the
// repository root.
const InventoryPath = "docs/feature-inventory.md"

// InventoryHeading opens the region whose tables are rows. The region ends at
// the next heading of the same level.
//
// The document declares the bound itself, and it needs one: its header carries
// six tables -- the check-to-column mapping, the token vocabulary, the format-set
// mapping, the column glossary, the evidence notation, the stability values --
// and the first of them has a column literally named `Feature ID`. An unbounded
// row scan cannot tell those from data, which is how the Phase 1 audit got three
// different answers to one question. internal/mcpserver/tool_doc_test.go bounds
// its own reading the same way and says so.
const InventoryHeading = "## The inventory"

// numberedSection matches the `### 1.` / `#### 1.1` subsections inside the
// region, for the diagnostic only. Nothing depends on the numbering, so the
// register can be reorganized without touching this package.
var numberedSection = regexp.MustCompile(`^#{3,6} (\d+)[. ]`)

// The columns every data table carries. Section 2 inserts an eleventh,
// `Strict-CE presence`, which is why rows are read by column NAME and never by
// position.
const (
	columnFeatureID = "Feature ID"
	columnSurface   = "Public surface"
)

// RequiredColumns are the fields stokaro/ptah#804 specifies. They are asserted
// by name so a table that lost one is a failure here rather than a field
// silently going unread.
var RequiredColumns = []string{
	columnFeatureID,
	columnSurface,
	"User goal",
	"Evidence",
	"Supported scope",
	"Stability",
	"Canonical page",
	"Example",
	"Visual",
	"Last verified revision",
}

// TokenKind says which surface a Public surface token names.
type TokenKind string

const (
	// KindCommand is a launcher-qualified command path: `ptah schema render`.
	KindCommand TokenKind = "command"
	// KindPackage is a Go import path from the public-API ledger.
	KindPackage TokenKind = "package"
	// KindProgram is a `main` package directory, written `./cmd/ptah-ls`.
	KindProgram TokenKind = "program"
	// KindFormat is one value of one enumerated set, written
	// `format:<list>/<value>`.
	KindFormat TokenKind = "format"
	// KindValue is any other backticked token: a flag, a file name, a scheme.
	// Rows write those into the same cell as prose, and reading them as a
	// surface claim would report every one of them as naming nothing.
	KindValue TokenKind = "value"
)

// Token is one surface an inventory row claims.
type Token struct {
	Kind TokenKind
	// Raw is the token exactly as the row spells it, for the diagnostic.
	Raw string
	// Tree and Path are set for KindCommand.
	Tree Tree
	Path string
	// Value is the import path, program directory, format value, or bare token.
	Value string
	// List is the format set name, set for KindFormat.
	List string
}

// Row is one inventory entry.
type Row struct {
	// Section is the numbered section the row was read from.
	Section int
	Line    int
	// ID is the stable identifier the checks key on.
	ID string
	// Surface holds the parsed tokens of the Public surface column.
	Surface []Token
	// Cells maps column name to contents.
	Cells map[string]string
}

// Inventory is the parsed document.
type Inventory struct {
	Path string
	Rows []Row
	// Sections lists the numbered sections that yielded rows, so a section that
	// stopped parsing is visible rather than merely absent.
	Sections []int
}

// formatToken matches `format:<list>/<value>`. The empty value is meaningful:
// one row records that the default of `migrations generate --report` is the
// empty string.
var formatToken = regexp.MustCompile(`^format:([a-z0-9-]+)/(.*)$`)

// backticked matches one backticked token.
var backticked = regexp.MustCompile("`([^`]+)`")

// LoadInventory reads and parses the inventory document.
func LoadInventory(repoRoot string) (*Inventory, error) {
	path := filepath.Join(repoRoot, filepath.FromSlash(InventoryPath))
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("featureinventory: reading %s: %w", InventoryPath, err)
	}

	inventory := &Inventory{Path: InventoryPath}
	seen := make(map[string]int)
	inside := false
	section := 0
	var columns []string

	for index, raw := range strings.Split(string(body), "\n") {
		line := strings.TrimSpace(raw)

		if strings.HasPrefix(line, "## ") {
			inside = line == InventoryHeading
			columns = nil
			section = 0
			continue
		}
		if strings.HasPrefix(line, "#") {
			// A subsection starts a new table, with its own header row.
			columns = nil
			if match := numberedSection.FindStringSubmatch(line); match != nil {
				section = atoi(match[1])
			}
			continue
		}
		if !inside || !strings.HasPrefix(line, "|") {
			continue
		}

		cells := tableCells(line)
		if isDelimiterRow(cells) {
			continue
		}
		if columns == nil {
			columns = cells
			// A table inside the region that is not keyed the way a surface row
			// is keyed is a different kind of table -- section 16's flag
			// baseline carries five columns of its own -- and is read by
			// nothing here rather than being forced into this shape.
			if isSurfaceTable(columns) {
				if err := checkColumns(section, index+1, columns); err != nil {
					return nil, err
				}
			}
			continue
		}
		if !isSurfaceTable(columns) {
			continue
		}

		row, err := newRow(section, index+1, columns, cells)
		if err != nil {
			return nil, err
		}
		if previous, duplicate := seen[row.ID]; duplicate {
			return nil, fmt.Errorf("%s:%d: feature ID %q is already used at line %d; an identifier the checks key on cannot name two features",
				InventoryPath, row.Line, row.ID, previous)
		}
		seen[row.ID] = row.Line
		inventory.Sections = appendUnique(inventory.Sections, section)
		inventory.Rows = append(inventory.Rows, row)
	}

	if len(inventory.Rows) == 0 {
		return nil, fmt.Errorf("featureinventory: %s yielded no rows under %q; a gate comparing a surface against an empty inventory reports success at exactly the moment it stopped working",
			InventoryPath, InventoryHeading)
	}
	return inventory, nil
}

// isSurfaceTable reports a table keyed the way a surface row is keyed.
func isSurfaceTable(columns []string) bool {
	return slices.Contains(columns, columnFeatureID) && slices.Contains(columns, columnSurface)
}

// newRow assembles one row, reading columns by name.
func newRow(section, line int, columns, cells []string) (Row, error) {
	row := Row{Section: section, Line: line, Cells: make(map[string]string)}
	for index, column := range columns {
		if index < len(cells) {
			row.Cells[column] = cells[index]
		}
	}
	row.ID = strings.Trim(strings.TrimSpace(row.Cells[columnFeatureID]), "`")
	if row.ID == "" {
		return Row{}, fmt.Errorf("%s:%d: an inventory row carries no feature ID", InventoryPath, line)
	}
	row.Surface = parseTokens(row.Cells[columnSurface])
	return row, nil
}

// checkColumns holds each data table to the fields the issue specifies.
func checkColumns(section, line int, columns []string) error {
	var missing []string
	for _, required := range RequiredColumns {
		if !slices.Contains(columns, required) {
			missing = append(missing, required)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s:%d: the table in section %d is missing the column(s) %s",
			InventoryPath, line, section, strings.Join(missing, ", "))
	}
	return nil
}

// parseTokens reads the typed tokens out of a Public surface cell.
//
// Three kinds identify themselves by their spelling. Everything else is carried
// through as a bare value, because the inventory writes a format set's accepted
// values into a sentence rather than into a prefixed token, and a check that
// demanded a prefix would be demanding the document be written for the check.
func parseTokens(cell string) []Token {
	var tokens []Token
	for _, match := range backticked.FindAllStringSubmatch(cell, -1) {
		raw := strings.TrimSpace(match[1])
		switch {
		case raw == "ptah" || strings.HasPrefix(raw, "ptah "):
			tokens = append(tokens, Token{Kind: KindCommand, Raw: raw, Tree: TreeNative, Path: strings.TrimSpace(strings.TrimPrefix(raw, "ptah"))})
		case raw == "ptah-compat" || strings.HasPrefix(raw, "ptah-compat "):
			tokens = append(tokens, Token{Kind: KindCommand, Raw: raw, Tree: TreeCompat, Path: strings.TrimSpace(strings.TrimPrefix(raw, "ptah-compat"))})
		case strings.HasPrefix(raw, "go.5x5.cz/ptah/"):
			tokens = append(tokens, Token{Kind: KindPackage, Raw: raw, Value: raw})
		case strings.HasPrefix(raw, "./cmd"):
			// A program claim is `./cmd/...`, never every `./`-prefixed token.
			// Rows write ordinary paths the same way -- `./.ptah/allowed_signers`
			// is a configuration file -- and reading those as programs reported
			// a config path as a `main` package that does not exist.
			tokens = append(tokens, Token{Kind: KindProgram, Raw: raw, Value: strings.TrimPrefix(raw, "./")})
		default:
			if match := formatToken.FindStringSubmatch(raw); match != nil {
				tokens = append(tokens, Token{Kind: KindFormat, Raw: raw, List: match[1], Value: match[2]})
				continue
			}
			tokens = append(tokens, Token{Kind: KindValue, Raw: raw, Value: raw})
		}
	}
	return tokens
}

// ValueTokens returns every bare token any row claims, as a set.
func (i *Inventory) ValueTokens() map[string]bool {
	values := make(map[string]bool)
	for _, row := range i.Rows {
		for _, token := range row.Surface {
			values[token.Raw] = true
		}
	}
	return values
}

// tableCells splits a Markdown table row into trimmed cells.
func tableCells(row string) []string {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(row, "|"), "|")
	parts := strings.Split(trimmed, "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cells = append(cells, strings.TrimSpace(part))
	}
	return cells
}

// isDelimiterRow reports the `| --- | --- |` separator.
func isDelimiterRow(cells []string) bool {
	for _, cell := range cells {
		if strings.Trim(cell, ":- ") != "" {
			return false
		}
	}
	return len(cells) > 0
}

// SortedIDs lists every feature ID.
func (i *Inventory) SortedIDs() []string {
	ids := make([]string, 0, len(i.Rows))
	for _, row := range i.Rows {
		ids = append(ids, row.ID)
	}
	sort.Strings(ids)
	return ids
}

// atoi parses a small non-negative integer, answering 0 for anything else.
func atoi(text string) int {
	value := 0
	for _, r := range text {
		if r < '0' || r > '9' {
			return 0
		}
		value = value*10 + int(r-'0')
	}
	return value
}

// appendUnique adds a section number once.
func appendUnique(list []int, value int) []int {
	if slices.Contains(list, value) {
		return list
	}
	return append(list, value)
}
