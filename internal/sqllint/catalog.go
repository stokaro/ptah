package sqllint

// CatalogRule is one identifier `ptah sql lint` can report.
//
// The SQL linter has no rule registry to enumerate: two of its identifiers come
// from [Rule] implementations and two are produced by the parse path, which
// runs before any rule sees a statement. This declaration is the enumeration,
// and it is checked from two sides rather than trusted. A test scans this
// package for rule-code constants and fails when one of them is missing here,
// and another drives the linter and fails when an emitted finding's title or
// severity differs from the row below.
type CatalogRule struct {
	// ID is the identifier a finding reports under.
	ID string
	// Title is the short label the finding carries.
	Title string
	// Severity is the severity the finding carries.
	Severity Severity
}

// catalogRules lists every identifier the linter can emit, in identifier order.
var catalogRules = []CatalogRule{
	{ID: RuleParseError, Title: "SQL parse error", Severity: SeverityError},
	{ID: RuleUnsupportedStatement, Title: "Unsupported SQL statement", Severity: SeverityError},
	{ID: RuleTableWithoutPrimaryKey, Title: "Table has no primary key", Severity: SeverityWarning},
	{ID: RuleUnsupportedCapability, Title: "Statement requires unsupported capability", Severity: SeverityError},
}

// Catalog returns every identifier `ptah sql lint` can report.
func Catalog() []CatalogRule {
	out := make([]CatalogRule, len(catalogRules))
	copy(out, catalogRules)
	return out
}

// CatalogIDs returns just the identifiers, in the same order as [Catalog].
func CatalogIDs() []string {
	ids := make([]string, 0, len(catalogRules))
	for _, rule := range catalogRules {
		ids = append(ids, rule.ID)
	}
	return ids
}

// CatalogTitle returns the short label for an identifier, or the empty string
// when the identifier is not one this linter reports.
func CatalogTitle(id string) string {
	for _, rule := range catalogRules {
		if rule.ID == id {
			return rule.Title
		}
	}
	return ""
}

// CatalogSeverity returns the severity for an identifier, or the empty string
// when the identifier is not one this linter reports.
func CatalogSeverity(id string) Severity {
	for _, rule := range catalogRules {
		if rule.ID == id {
			return rule.Severity
		}
	}
	return ""
}
