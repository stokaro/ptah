package typeorder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/deporder"
	"ptah.run/internal/typeorder"
)

func TestRebuildNamesFollowsResolvedTypeReferences(t *testing.T) {
	for _, test := range []struct {
		name                 string
		definitions          []deporder.UserType
		changed, seeds, want []string
	}{
		{name: "transitive changed dependents", definitions: []deporder.UserType{{Name: "a.base"}, {Name: "a.span", References: []string{"a.base"}}, {Name: "a.wrapper", References: []string{"a.span"}}}, changed: []string{"a.base", "a.span", "a.wrapper"}, seeds: []string{"a.base"}, want: []string{"a.base", "a.span", "a.wrapper"}},
		{name: "different schemas", definitions: []deporder.UserType{{Name: "a.base"}, {Name: "b.base"}, {Name: "b.span", References: []string{"b.base"}}}, changed: []string{"a.base", "b.span"}, seeds: []string{"a.base"}, want: []string{"a.base"}},
		{name: "quoted case", definitions: []deporder.UserType{{Name: `app."Base"`}, {Name: "app.base"}, {Name: "app.span", References: []string{`app."Base"[]`}}}, changed: []string{`app."Base"`, "app.span"}, seeds: []string{`app."Base"`}, want: []string{`app."Base"`, "app.span"}},
		{name: "unchanged dependent remains a blocker", definitions: []deporder.UserType{{Name: "base"}, {Name: "span", References: []string{"base"}}, {Name: "wrapper", References: []string{"span"}}}, changed: []string{"base", "wrapper"}, seeds: []string{"base"}, want: []string{"base"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := typeorder.RebuildNames(test.definitions, test.changed, test.seeds)
			want := make(map[string]struct{})
			for _, name := range test.want {
				want[name] = struct{}{}
			}
			c.Assert(got, qt.DeepEquals, want)
		})
	}
}
