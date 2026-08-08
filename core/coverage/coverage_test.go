package coverage_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
)

// TestZeroSetDescribesEverything is the floor the whole design rests on: a
// description that says nothing about its own limits is fully authoritative, so
// adding coverage to the IR changes no plan for any schema file anyone has
// already written.
func TestZeroSetDescribesEverything(t *testing.T) {
	c := qt.New(t)

	var set coverage.Set

	c.Assert(set.IsZero(), qt.IsTrue)
	c.Assert(set.Describes(coverage.Extension, "pgcrypto"), qt.IsTrue)
	c.Assert(set.Describes(coverage.Role, "admin_user"), qt.IsTrue)
	c.Assert(set.DescribesSchema("extra"), qt.IsTrue)
	c.Assert(set.DescribesIn(coverage.Sequence, "extra", "extra.order_seq"), qt.IsTrue)
}

func TestSetDescribes_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		set   coverage.Set
		check func(c *qt.C, set coverage.Set)
	}{
		{
			name: "a whole undescribed kind hides every object of it",
			set:  coverage.Set{}.WithKind(coverage.Extension),
			check: func(c *qt.C, set coverage.Set) {
				c.Assert(set.Describes(coverage.Extension, "pgcrypto"), qt.IsFalse)
				c.Assert(set.Describes(coverage.Extension, "postgis"), qt.IsFalse)
			},
		},
		{
			name: "an undescribed kind leaves every other kind authoritative",
			set:  coverage.Set{}.WithKind(coverage.Extension),
			check: func(c *qt.C, set coverage.Set) {
				c.Assert(set.Describes(coverage.Sequence, "order_seq"), qt.IsTrue)
				c.Assert(set.Describes(coverage.Role, "pgcrypto"), qt.IsTrue)
			},
		},
		{
			name: "an undescribed object hides only itself",
			set:  coverage.Set{}.WithObject(coverage.Role, "admin_user"),
			check: func(c *qt.C, set coverage.Set) {
				c.Assert(set.Describes(coverage.Role, "admin_user"), qt.IsFalse)
				c.Assert(set.Describes(coverage.Role, "app_user"), qt.IsTrue)
			},
		},
		{
			name: "any offered spelling of an undescribed object matches",
			set:  coverage.Set{}.WithObject(coverage.Sequence, "extra.order_seq"),
			check: func(c *qt.C, set coverage.Set) {
				c.Assert(set.Describes(coverage.Sequence, "extra.order_seq", "order_seq"), qt.IsFalse)
				c.Assert(set.Describes(coverage.Sequence, "order_seq"), qt.IsTrue)
			},
		},
		{
			name: "matching is case-insensitive, as unquoted identifiers are",
			set:  coverage.Set{}.WithObject(coverage.Extension, "PgCrypto"),
			check: func(c *qt.C, set coverage.Set) {
				c.Assert(set.Describes(coverage.Extension, "pgcrypto"), qt.IsFalse)
			},
		},
		{
			name: "an undescribed schema hides everything in it",
			set:  coverage.Set{}.WithObject(coverage.Schema, "extra"),
			check: func(c *qt.C, set coverage.Set) {
				c.Assert(set.DescribesSchema("extra"), qt.IsFalse)
				c.Assert(set.DescribesIn(coverage.Sequence, "extra", "extra.order_seq"), qt.IsFalse)
				c.Assert(set.DescribesIn(coverage.Sequence, "public", "public.order_seq"), qt.IsTrue)
			},
		},
		{
			name: "an object with no owning schema is not hidden by a schema record",
			set:  coverage.Set{}.WithObject(coverage.Schema, "extra"),
			check: func(c *qt.C, set coverage.Set) {
				c.Assert(set.DescribesIn(coverage.Extension, "", "pgcrypto"), qt.IsTrue)
			},
		},
		{
			name: "merging unions both descriptions' limits",
			set: coverage.Set{}.WithKind(coverage.Policy).
				Merge(coverage.Set{}.WithObject(coverage.Role, "admin_user")),
			check: func(c *qt.C, set coverage.Set) {
				c.Assert(set.Describes(coverage.Policy, "p"), qt.IsFalse)
				c.Assert(set.Describes(coverage.Role, "admin_user"), qt.IsFalse)
				c.Assert(set.Describes(coverage.Extension, "pgcrypto"), qt.IsTrue)
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			test.check(c, test.set)
		})
	}
}

// TestNormalizeIsDeterministic pins the ordering the serialized form depends on.
// The record rides in a generated document, and a document whose bytes depend on
// map or insertion order is one nobody can diff.
func TestNormalizeIsDeterministic(t *testing.T) {
	c := qt.New(t)

	set := coverage.Set{
		Kinds: []coverage.Kind{coverage.Sequence, coverage.Extension, coverage.Sequence},
		Objects: []coverage.Object{
			{Kind: coverage.Schema, Name: "extra"},
			{Kind: coverage.Role, Name: "b"},
			{Kind: coverage.Role, Name: "a"},
			{Kind: coverage.Role, Name: "b"},
		},
	}.Normalize()

	c.Assert(set.Kinds, qt.DeepEquals, []coverage.Kind{coverage.Extension, coverage.Sequence})
	c.Assert(set.Objects, qt.DeepEquals, []coverage.Object{
		{Kind: coverage.Role, Name: "a"},
		{Kind: coverage.Role, Name: "b"},
		{Kind: coverage.Schema, Name: "extra"},
	})
}

func TestDirectivesRoundTrip_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		set  coverage.Set
		want []string
	}{
		{
			name: "a whole kind",
			set:  coverage.Set{}.WithKind(coverage.Extension),
			want: []string{"ptah:not-described extension"},
		},
		{
			name: "one object, quoted",
			set:  coverage.Set{}.WithObject(coverage.Schema, "extra"),
			want: []string{`ptah:not-described schema "extra"`},
		},
		{
			name: "kinds before objects, each sorted",
			set: coverage.Set{}.
				WithKind(coverage.Sequence, coverage.Extension).
				WithObject(coverage.Schema, "extra"),
			want: []string{
				"ptah:not-described extension",
				"ptah:not-described sequence",
				`ptah:not-described schema "extra"`,
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(test.set.Directives(), qt.DeepEquals, test.want)

			// The directive lines are the only channel between the process that
			// renders a document and the process that reads it back, so what
			// they encode has to survive being written and read again.
			var document strings.Builder
			for _, directive := range test.set.Directives() {
				fmt.Fprintf(&document, "// %s\n", directive)
			}
			decoded, err := coverage.DecodeHeader(document.String())
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, test.set)
		})
	}
}

func TestDecodeHeader_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		document string
		want     coverage.Set
	}{
		{
			name:     "no directives at all is full authority",
			document: "// Code generated by ptah; DO NOT EDIT.\n\nschema \"public\" {\n}\n",
			want:     coverage.Set{},
		},
		{
			name: "every comment spelling a schema document can use",
			document: "// ptah:not-described extension\n" +
				"# ptah:not-described policy\n" +
				"-- ptah:not-described sequence\n" +
				"schema \"public\" {}\n",
			want: coverage.Set{}.WithKind(coverage.Extension, coverage.Policy, coverage.Sequence),
		},
		{
			name: "blank lines inside the header do not end it",
			document: "// Code generated by ptah; DO NOT EDIT.\n" +
				"\n" +
				"// ptah:not-described extension\n" +
				"\n" +
				"schema \"public\" {}\n",
			want: coverage.Set{}.WithKind(coverage.Extension),
		},
		{
			name: "a directive below the first content line is not a directive",
			document: "schema \"public\" {}\n" +
				"// ptah:not-described extension\n",
			want: coverage.Set{},
		},
		{
			// A line inside a heredoc can begin with anything, including this
			// marker. It is not a comment and it is not in the header, and
			// either test alone would let it through: a decoder that accepted
			// any line starting with the marker would read it, and so would one
			// that scanned every comment in the file rather than stopping.
			name: "a directive inside a multi-line value is not a directive",
			document: "// Code generated by ptah; DO NOT EDIT.\n\n" +
				"table \"t\" {\n  comment = <<-EOT\nptah:not-described extension\nEOT\n}\n",
			want: coverage.Set{},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := coverage.DecodeHeader(test.document)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestDecodeHeader_FailurePath pins that a directive this build cannot read is
// refused rather than passed over. Passing over it is the failure this package
// exists to prevent: an unread record reads as no record, and the absence it was
// protecting becomes a removal.
func TestDecodeHeader_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name:     "unknown kind",
			document: "// ptah:not-described publication\n",
			wantErr:  `unknown coverage kind "publication": valid kinds are composite, domain, extension, policy, range, role, schema, sequence`,
		},
		{
			name:     "no kind",
			document: "// ptah:not-described\n",
			wantErr:  `malformed ptah:not-described directive "ptah:not-described": expected a kind and an optional quoted name`,
		},
		{
			name:     "too many fields",
			document: `// ptah:not-described schema "extra" "more"` + "\n",
			wantErr:  `malformed ptah:not-described directive .*: expected a kind and an optional quoted name`,
		},
		{
			name:     "unquoted name",
			document: "// ptah:not-described schema extra\n",
			wantErr:  `malformed ptah:not-described directive "ptah:not-described schema extra": name must be a quoted string`,
		},
		{
			name:     "empty name",
			document: `// ptah:not-described schema ""` + "\n",
			wantErr:  `malformed ptah:not-described directive .*: name must not be empty`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := coverage.DecodeHeader(test.document)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got.IsZero(), qt.IsTrue)
		})
	}
}

func TestParseKind_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := coverage.ParseKind("table")
	c.Assert(err, qt.ErrorMatches, `unknown coverage kind "table": valid kinds are .*`)
	c.Assert(got, qt.Equals, coverage.Kind(""))
}
