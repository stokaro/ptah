package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// spelled is a descriptor whose identifiers are written by quoting and whose
// column keys are named by naming.
func spelled(quoting descriptor.Quoting, naming descriptor.ColumnKeyNaming) descriptor.Dialect {
	described := head("alpha")
	described.Quoting = quoting
	described.ColumnKeys = naming
	return described
}

// folded is a descriptor that records an unquoted name in the case fold
// names.
func folded(fold string) descriptor.Dialect {
	described := head("alpha")
	described.Identifiers.UnquotedFold = fold
	return described
}

// caseKept is a descriptor whose SQL text keeps the case of a quoted name and
// whose catalog records an unquoted name in the case fold names.
func caseKept(fold string) descriptor.Dialect {
	described := folded(fold)
	described.Comparison.QuotedNamesKeepCase = true
	return described
}

// bare is a descriptor whose unquoted names may hold chars besides letters,
// digits and underscores.
func bare(chars string) descriptor.Dialect {
	described := head("alpha")
	described.Identifiers.BareChars = chars
	return described
}

// TestNewRegistry_BareChars_HappyPath keeps the bare characters a descriptor
// lists.
func TestNewRegistry_BareChars_HappyPath(t *testing.T) {
	c := qt.New(t)

	registry, err := descriptor.NewRegistry(bare("$#"))

	c.Assert(err, qt.IsNil)
	got, found := registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(got.Identifiers.BareChars, qt.Equals, "$#")
}

// TestNewRegistry_UnquotedFold_HappyPath accepts every declared fold and keeps
// it on the registered descriptor.
func TestNewRegistry_UnquotedFold_HappyPath(t *testing.T) {
	for _, fold := range append([]string{""}, descriptor.UnquotedFolds()...) {
		t.Run(fold, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(folded(fold))

			c.Assert(err, qt.IsNil)
			got, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(got.Identifiers.UnquotedFold, qt.Equals, fold)
		})
	}
}

func TestNewRegistry_IdentifierFields_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect descriptor.Dialect
	}{
		{name: "the standard spelling", dialect: spelled(descriptor.Quoting{}, "")},
		{name: "every declared value", dialect: spelled(descriptor.Quoting{
			Style: descriptor.QuoteYQL, Bare: descriptor.BareNever, Keywords: []string{"select"}, Paths: true,
		}, descriptor.ColumnKeyTableColumnKey)},
		{name: "bare names everywhere", dialect: spelled(descriptor.Quoting{
			Style: descriptor.QuoteBracket, Bare: descriptor.BareEverywhere,
		}, descriptor.ColumnKeyAfterColumn)},
		{name: "quoted names keep case with no fold", dialect: caseKept("")},
		{name: "quoted names keep case with the lower fold", dialect: caseKept("lower")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialect)

			c.Assert(err, qt.IsNil)
			got, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(got.Quoting, qt.DeepEquals, test.dialect.Quoting)
			c.Assert(got.ColumnKeys, qt.Equals, test.dialect.ColumnKeys)
		})
	}
}

func TestNewRegistry_IdentifierFields_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect descriptor.Dialect
		wantErr string
	}{
		{name: "an undeclared quote style", dialect: spelled(descriptor.Quoting{Style: "single"}, ""),
			wantErr: `invalid dialect descriptor: dialect "alpha": quote style "single" is not one of .*`},
		{name: "an undeclared bare policy", dialect: spelled(descriptor.Quoting{Bare: "sometimes"}, ""),
			wantErr: `invalid dialect descriptor: dialect "alpha": bare names "sometimes" is not one of .*`},
		{name: "an empty keyword", dialect: spelled(descriptor.Quoting{Keywords: []string{"select", ""}}, ""),
			wantErr: `invalid dialect descriptor: dialect "alpha": an empty keyword`},
		{name: "an undeclared column key naming", dialect: spelled(descriptor.Quoting{}, "after-table"),
			wantErr: `invalid dialect descriptor: dialect "alpha": column key naming "after-table" is not one of .*`},
		{name: "a letter among the bare characters", dialect: bare("$a"),
			wantErr: `invalid dialect descriptor: dialect "alpha": bare character 'a' is not ASCII punctuation outside quotes and dots`},
		{name: "a quote among the bare characters", dialect: bare(`$"`),
			wantErr: `invalid dialect descriptor: dialect "alpha": bare character '"' is not ASCII punctuation outside quotes and dots`},
		{name: "a dot among the bare characters", dialect: bare("."),
			wantErr: `invalid dialect descriptor: dialect "alpha": bare character '\.' is not ASCII punctuation outside quotes and dots`},
		{name: "an undeclared unquoted fold", dialect: folded("title"),
			wantErr: `invalid dialect descriptor: dialect "alpha": unquoted fold "title" is not one of \["upper" "lower"\]`},
		{name: "quoted names keep case with the upper fold", dialect: caseKept("upper"),
			wantErr: `invalid dialect descriptor: dialect "alpha": quoted names keep case folds a bare name to lower case, and unquoted fold "upper" folds it to upper case`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialect)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}
