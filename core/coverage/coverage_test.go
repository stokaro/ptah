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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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

// TestDirectivesRoundTripAdversarialNames pins the writer against the reader
// over every name shape a quoted identifier is allowed to have.
//
// The first row is the defect this test exists for: the name was written with
// strconv.Quote and read back with strings.Fields, which splits on whitespace
// and knows nothing about quotes. `schema "extra reports"` went out as two
// tokens and came back as three, so Ptah refused a header Ptah had just
// written, and every legal identifier containing a space was unrepresentable
// (stokaro/ptah#1276). A newline, a tab and a control character are here for
// the other half of the contract: the encoding is line-based, so a name must
// never be able to end its own comment.
func TestDirectivesRoundTripAdversarialNames(t *testing.T) {
	tests := []struct {
		name  string
		given string
		want  string
	}{
		{name: "embedded space", given: "extra reports", want: "extra reports"},
		{name: "several embedded spaces", given: "a  b   c", want: "a  b   c"},
		{name: "embedded double quote", given: `we"ird`, want: `we"ird`},
		{name: "embedded backslash", given: `back\slash`, want: `back\slash`},
		{name: "backslash before a quote", given: `back\"both`, want: `back\"both`},
		{name: "embedded newline", given: "two\nlines", want: "two\nlines"},
		{name: "embedded tab", given: "two\ttabs", want: "two\ttabs"},
		{name: "embedded carriage return", given: "two\rparts", want: "two\rparts"},
		{name: "unicode", given: "naïve schéma", want: "naïve schéma"},
		{name: "non-breaking space", given: "a\u00a0b", want: "a\u00a0b"},
		{name: "the directive marker as a name", given: "ptah:not-described", want: "ptah:not-described"},
		{name: "a comment prefix as a name", given: "// -- #", want: "// -- #"},
		{name: "surrounding whitespace is trimmed", given: "  padded  ", want: "padded"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set := coverage.Set{}.WithObject(coverage.Schema, test.given)

			directives := set.Directives()
			c.Assert(directives, qt.HasLen, 1)

			// A directive is one line of a comment header. A name that could
			// break out of its line would end the header early, and every
			// record after it would be read as file content.
			c.Assert(directives[0], qt.Not(qt.Contains), "\n")
			c.Assert(directives[0], qt.Not(qt.Contains), "\r")

			decoded, err := coverage.DecodeHeader("// " + directives[0] + "\n")
			c.Assert(err, qt.IsNil)
			c.Assert(decoded.Objects, qt.DeepEquals, []coverage.Object{
				{Kind: coverage.Schema, Name: test.want},
			})
			c.Assert(decoded, qt.DeepEquals, set)

			// The point of the round trip: what came back still protects the
			// object the writer meant to protect.
			c.Assert(decoded.DescribesSchema(test.want), qt.IsFalse)
			c.Assert(decoded.DescribesSchema("something else"), qt.IsTrue)
		})
	}
}

// TestDirectivesNeverWriteALineDecodeRefuses is the contract the empty name sits
// on. A record naming nothing cannot be serialized -- DecodeHeader refuses an
// empty quoted name deliberately, so a hand-edited document cannot smuggle one
// in -- and dropping it would widen what the description claims to cover, which
// is the destructive direction. It is promoted to a record about the whole kind:
// the conservative superset, written into the document where a reader can see
// it.
func TestDirectivesNeverWriteALineDecodeRefuses(t *testing.T) {
	tests := []struct {
		name string
		set  coverage.Set
	}{
		{
			name: "an empty name",
			set:  coverage.Set{}.WithObject(coverage.Schema, ""),
		},
		{
			name: "a name that is only whitespace",
			set:  coverage.Set{}.WithObject(coverage.Extension, " \t "),
		},
		{
			name: "a nameless object in a hand-built set",
			set:  coverage.Set{Objects: []coverage.Object{{Kind: coverage.Sequence}}}.Normalize(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var document strings.Builder
			for _, directive := range test.set.Directives() {
				fmt.Fprintf(&document, "// %s\n", directive)
			}

			decoded, err := coverage.DecodeHeader(document.String())
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, test.set)
			c.Assert(decoded.Objects, qt.HasLen, 0)
			c.Assert(decoded.Kinds, qt.HasLen, 1)
		})
	}
}

func TestDecodeHeader_HappyPath(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
			// The name is decoded from the whole remainder of the line, so a
			// second quoted token is trailing garbage inside the name rather
			// than an extra whitespace-delimited field. strconv.Unquote refuses
			// it, which is what keeps the grammar one kind plus at most one
			// name now that a name may itself contain spaces.
			name:     "trailing text after the name",
			document: `// ptah:not-described schema "extra" "more"` + "\n",
			wantErr:  `malformed ptah:not-described directive .*: name must be a quoted string`,
		},
		{
			name:     "unterminated quote",
			document: `// ptah:not-described schema "extra` + "\n",
			wantErr:  `malformed ptah:not-described directive .*: name must be a quoted string`,
		},
		{
			name:     "a back-quoted name is not a spelling this package writes",
			document: "// ptah:not-described schema `extra`\n",
			wantErr:  `malformed ptah:not-described directive .*: name must be a quoted string`,
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
