package atlasschema

// White-box testing required: declaredConcurrentIndexRefs is unexported, and
// the three answers it distinguishes are invisible from outside -- two of them
// produce the same statements through Diff, and the third differs only on a
// target the offline suite cannot reach.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func concurrentDesired() *goschema.Database {
	return &goschema.Database{
		Tables:  []goschema.Table{{StructName: "Widget", Name: "widget"}},
		Indexes: []goschema.Index{{StructName: "Widget", Name: "idx_widget_a", Fields: []string{"a"}, Concurrently: true}},
	}
}

func concurrentDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		IndexesAdded: []difftypes.IndexRef{{Name: "idx_widget_a", TableName: "widget"}},
	}
}

// TestDeclaredConcurrentIndexRefs_TheModeDecides pins the three answers, of
// which the middle one is the fix for stokaro/ptah#2019 and the last one is the
// reason the policy needed a third field.
//
// ConcurrentIndexCreate's zero value is not "no". It means nothing was
// requested, and a description that asked for `CREATE INDEX CONCURRENTLY` is
// still honored. Only an explicit instruction turns that off.
func TestDeclaredConcurrentIndexRefs_TheModeDecides(t *testing.T) {
	tests := []struct {
		name   string
		policy DiffPolicy
		want   int
		why    string
	}{
		{
			name:   "nothing requested honors the declaration",
			policy: DiffPolicy{},
			want:   1,
			why:    "the desired state asked for the non-locking build and nobody said otherwise",
		},
		{
			name:   "the operator turned it off",
			policy: DiffPolicy{ConcurrentIndexCreateDisabled: true},
			want:   0,
			why:    "an instruction is not overruled by a description",
		},
		{
			name:   "the operator turned it on for everything",
			policy: DiffPolicy{ConcurrentIndexCreate: true},
			want:   0,
			why:    "the blanket option already covers this addition; naming it again would be redundant",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			refs := declaredConcurrentIndexRefs(
				test.policy, concurrentDiff(), concurrentDesired(), nil,
				"postgres", capability.ForDialect("postgres"),
			)

			c.Assert(refs, qt.HasLen, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestDeclaredConcurrentIndexRefs_ResolvesTheDialectDefault is the trap this
// helper exists to avoid.
//
// A caller that pinned no server version passes nil capabilities, which is the
// planner's own "use the dialect default". Reading that nil through
// Capabilities.Has answers false for every key, so the declaration would go
// silently unhonored on exactly the invocation that pins no version -- the
// common one. The nil is resolved here so the two layers agree.
func TestDeclaredConcurrentIndexRefs_ResolvesTheDialectDefault(t *testing.T) {
	c := qt.New(t)

	refs := declaredConcurrentIndexRefs(
		DiffPolicy{}, concurrentDiff(), concurrentDesired(), nil, "postgres", nil,
	)

	c.Assert(refs, qt.HasLen, 1,
		qt.Commentf("nil capabilities must resolve to the dialect default, not read as an absent capability"))
}

// TestDeclaredConcurrentIndexRefs_UnknownDialectStaysPlain is the control for
// the resolution above: it must resolve a dialect Ptah knows, not answer true
// for every nil.
func TestDeclaredConcurrentIndexRefs_UnknownDialectStaysPlain(t *testing.T) {
	c := qt.New(t)

	refs := declaredConcurrentIndexRefs(
		DiffPolicy{}, concurrentDiff(), concurrentDesired(), nil, "nosuchengine", nil,
	)

	c.Assert(refs, qt.HasLen, 0)
}
