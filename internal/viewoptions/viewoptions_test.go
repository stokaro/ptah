package viewoptions_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/viewoptions"
)

// The spellings on the left are the ones PostgreSQL accepts in a WITH clause
// and the ones its catalog reports back: measured on 15.19 and 18.6,
// `WITH (security_invoker = true, security_barrier)` is stored as
// {security_invoker=true,security_barrier=true}.
func TestNormalize_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		options []string
		want    []string
	}{
		{name: "nothing declared", options: nil, want: nil},
		{name: "an empty list is nothing declared", options: make([]string, 0), want: nil},
		{name: "a name and a value", options: []string{"security_invoker=true"}, want: []string{"security_invoker=true"}},
		{name: "a bare name is true", options: []string{"security_barrier"}, want: []string{"security_barrier=true"}},
		{name: "the name is folded and trimmed", options: []string{" Security_Invoker = on "}, want: []string{"security_invoker=on"}},
		{name: "a string literal is unquoted", options: []string{"fillfactor='070'"}, want: []string{"fillfactor=070"}},
		{name: "a doubled quote inside a literal is one quote", options: []string{"x_y='it''s'"}, want: []string{"x_y=it's"}},
		{
			name:    "the order declared is kept",
			options: []string{"security_invoker=true", "security_barrier=false"},
			want:    []string{"security_invoker=true", "security_barrier=false"},
		},
		{name: "a name may carry digits after its first letter", options: []string{"autovacuum_vacuum_scale_factor=0.2"}, want: []string{"autovacuum_vacuum_scale_factor=0.2"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := viewoptions.Normalize(test.options)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestNormalize_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		options []string
		wantErr string
	}{
		{name: "an empty name", options: []string{"=true"}, wantErr: `invalid view option: "=true" is not an option name: .*`},
		{name: "a name with a space", options: []string{"security invoker=true"}, wantErr: `invalid view option: "security invoker=true" is not an option name: .*`},
		{name: "a name starting with a digit", options: []string{"1x=2"}, wantErr: `invalid view option: "1x=2" is not an option name: .*`},
		{name: "a name given twice", options: []string{"fillfactor=70", "FILLFACTOR=80"}, wantErr: `invalid view option: "fillfactor" is given more than once`},
		{name: "no value after the equals sign", options: []string{"fillfactor="}, wantErr: `invalid view option: fillfactor has no value after =`},
		{name: "an empty literal", options: []string{"fillfactor=''"}, wantErr: `invalid view option: fillfactor has no value after =`},
		{name: "the check option", options: []string{"check_option=local"}, wantErr: `invalid view option: check_option is a view's check option; declare it as WITH CHECK OPTION`},
		{name: "a TOAST option", options: []string{"toast.autovacuum_enabled=false"}, wantErr: `invalid view option: toast.autovacuum_enabled is kept on the TOAST table, .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := viewoptions.Normalize(test.options)
			c.Assert(err, qt.ErrorIs, viewoptions.ErrInvalidOption)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// The left side of each row is what PostgreSQL 18.6 stored for the spelling a
// statement wrote, and the right side is a declaration of the same value.
func TestEqual_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		held      []string
		declared  []string
		wantEqual bool
	}{
		{name: "nothing on either side", held: nil, declared: make([]string, 0), wantEqual: true},
		{name: "the same options in another order", held: []string{"security_barrier=true", "security_invoker=true"},
			declared: []string{"security_invoker=true", "security_barrier=true"}, wantEqual: true},
		{name: "on is true", held: []string{"security_invoker=on"}, declared: []string{"security_invoker=true"}, wantEqual: true},
		{name: "yes is true", held: []string{"security_barrier=yes"}, declared: []string{"security_barrier"}, wantEqual: true},
		{name: "1 is true", held: []string{"security_invoker=1"}, declared: []string{"security_invoker=TRUE"}, wantEqual: true},
		{name: "off is false", held: []string{"autovacuum_enabled=off"}, declared: []string{"autovacuum_enabled=false"}, wantEqual: true},
		{name: "a leading zero is the same number", held: []string{"fillfactor=070"}, declared: []string{"fillfactor=70"}, wantEqual: true},
		{name: "an enumerated value in another case", held: []string{"vacuum_index_cleanup=auto"}, declared: []string{"vacuum_index_cleanup=AUTO"}, wantEqual: true},
		{name: "true is not false", held: []string{"security_invoker=true"}, declared: []string{"security_invoker=false"}, wantEqual: false},
		{name: "another number", held: []string{"fillfactor=70"}, declared: []string{"fillfactor=80"}, wantEqual: false},
		{name: "an option the database holds and the declaration does not", held: []string{"security_invoker=true"}, declared: nil, wantEqual: false},
		{name: "an option the declaration adds", held: nil, declared: []string{"security_barrier"}, wantEqual: false},
		{name: "another option of the same count", held: []string{"security_barrier=true"}, declared: []string{"security_invoker=true"}, wantEqual: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(viewoptions.Equal(test.held, test.declared), qt.Equals, test.wantEqual)
			c.Assert(viewoptions.Equal(test.declared, test.held), qt.Equals, test.wantEqual)
		})
	}
}

// A list Normalize refuses is equal to itself and to nothing else, so a
// comparison never reads two refused lists as the same declaration.
func TestEqual_FailurePath(t *testing.T) {
	c := qt.New(t)
	refused := []string{"toast.fillfactor=1"}
	c.Assert(viewoptions.Equal(refused, refused), qt.IsTrue)
	c.Assert(viewoptions.Equal(refused, nil), qt.IsFalse)
	c.Assert(viewoptions.Equal(refused, []string{"toast.fillfactor=2"}), qt.IsFalse)
}

func TestChanges_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		held      []string
		declared  []string
		wantSet   []string
		wantReset []string
	}{
		{name: "nothing to change", held: []string{"security_invoker=on"}, declared: []string{"security_invoker=true"}},
		{name: "an option added", held: nil, declared: []string{"security_barrier"}, wantSet: []string{"security_barrier=true"}},
		{name: "an option removed", held: []string{"security_invoker=true"}, declared: nil, wantReset: []string{"security_invoker"}},
		{name: "a value changed", held: []string{"fillfactor=70"}, declared: []string{"fillfactor='80'"}, wantSet: []string{"fillfactor=80"}},
		{
			name:      "one of each, in the order each side gives",
			held:      []string{"autovacuum_enabled=off", "fillfactor=70", "parallel_workers=2"},
			declared:  []string{"parallel_workers=4", "fillfactor=70", "autovacuum_vacuum_threshold=50"},
			wantSet:   []string{"parallel_workers=4", "autovacuum_vacuum_threshold=50"},
			wantReset: []string{"autovacuum_enabled"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			set, reset := viewoptions.Changes(test.held, test.declared)
			c.Assert(set, qt.DeepEquals, test.wantSet)
			c.Assert(reset, qt.DeepEquals, test.wantReset)
		})
	}
}

// A refused declaration changes nothing: the comparison refuses it before a
// plan reads the change.
func TestChanges_FailurePath(t *testing.T) {
	c := qt.New(t)
	set, reset := viewoptions.Changes([]string{"fillfactor=70"}, []string{"check_option=local"})
	c.Assert(set, qt.IsNil)
	c.Assert(reset, qt.IsNil)
}

func TestDescribe(t *testing.T) {
	tests := []struct {
		name    string
		options []string
		want    string
	}{
		{name: "nothing", options: nil, want: "(none)"},
		{name: "sorted by name", options: []string{"security_invoker=true", "security_barrier=true"}, want: "security_barrier=true, security_invoker=true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(viewoptions.Describe(test.options), qt.Equals, test.want)
		})
	}
}
