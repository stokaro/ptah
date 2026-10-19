package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// everywhere is a descriptor stating one construct of Reach.Everywhere for
// every part, each at a value the checks accept.
func everywhere() descriptor.Dialect {
	described := head("alpha")
	construct := func(name string) descriptor.ReachConstruct {
		return descriptor.ReachConstruct{Construct: name, Reach: "reaches out"}
	}
	starts, keyword, after := construct("starts"), construct("keyword"), construct("string after")
	starts.StartsWith = []string{"EXPORT", "TO"}
	keyword.Keyword = "REMOTE_FDW"
	after.StartsWith, after.StringAfter = []string{"EXPORT"}, []string{"TO", "INTO"}
	calls, prefixes, procedures := construct("calls"), construct("call prefixes"), construct("procedures")
	calls.Calls = []string{"FETCH_URL", "FETCH_FILE"}
	prefixes.CallPrefixes = []string{"FS_"}
	procedures.Procedures = []string{"XP_FETCH"}
	pragmas, words, engines, path := construct("pragmas"), construct("words then string"), construct("engines"), construct("path option")
	pragmas.Pragmas = []string{"SPOOL_DIRECTORY"}
	words.WordsThenString = []string{"INTO", "SPOOLFILE"}
	engines.Engines = []string{"REMOTE"}
	path.PathOption = []string{"SPOOL", "DIRECTORY"}
	described.Reach.Everywhere = []descriptor.ReachConstruct{
		starts, keyword, after, calls, prefixes, procedures, pragmas, words, engines, path,
	}
	return described
}

func TestNewRegistry_ReachEverywhere_HappyPath(t *testing.T) {
	c := qt.New(t)
	described := everywhere()

	registry, err := descriptor.NewRegistry(described)

	c.Assert(err, qt.IsNil)
	got, found := registry.Lookup("alpha")
	c.Assert(found, qt.IsTrue)
	c.Assert(got, qt.DeepEquals, described)
}

func TestNewRegistry_ReachEverywhere_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		edit    descriptor.Dialect
		wantErr string
	}{
		{
			name:    "no construct",
			edit:    withEverywhere(func(c *descriptor.ReachConstruct) { c.Construct = "" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach construct "" states no construct or no reach`,
		},
		{
			name:    "no reach",
			edit:    withEverywhere(func(c *descriptor.ReachConstruct) { c.Reach = "" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach construct "starts" states no construct or no reach`,
		},
		{
			name:    "no part",
			edit:    withEverywhere(func(c *descriptor.ReachConstruct) { c.StartsWith = nil }),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach construct "starts" states no part a statement could match`,
		},
		{
			name:    "a path option of one keyword",
			edit:    withEverywhere(func(c *descriptor.ReachConstruct) { c.PathOption = []string{"SPOOL"} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach construct "starts" states a path option \["SPOOL"\], which is not two keywords`,
		},
		{
			name:    "a keyword in lower case",
			edit:    withEverywhere(func(c *descriptor.ReachConstruct) { c.Keyword = "remote_fdw" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach construct "starts" states "remote_fdw", which is empty or not in upper case`,
		},
		{
			name:    "an empty name",
			edit:    withEverywhere(func(c *descriptor.ReachConstruct) { c.Calls = []string{""} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach construct "starts" states "", which is empty or not in upper case`,
		},
		{
			name:    "a name in lower case in a list",
			edit:    withEverywhere(func(c *descriptor.ReachConstruct) { c.Engines = []string{"REMOTE", "federated"} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach construct "starts" states "federated", which is empty or not in upper case`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.edit)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}

// withEverywhere returns everywhere with one change applied to its first
// construct, so each row carries the one value it is about.
func withEverywhere(change func(*descriptor.ReachConstruct)) descriptor.Dialect {
	described := everywhere()
	change(&described.Reach.Everywhere[0])
	return described
}
