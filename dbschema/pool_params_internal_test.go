package dbschema

// White-box testing required: removePostgresPoolParams is unexported and sits
// between ConnectToDatabase and the driver, so the only exported path to it
// opens a real connection. The property under test is what the URL looks like
// when it reaches the driver, which no exported result reports -- a corrupted
// one is visible solely as a server error about something the caller never
// wrote.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestRemovePostgresPoolParams_KeepsEveryOtherParameterByte is the regression.
//
// Encoding a query through url.Values spells a space as "+", which is right for
// a form and wrong for a libpq value read as content. `options=-c%20search_path
// %3Dextra` came back as `options=-c+search_path%3Dextra`, and PostgreSQL then
// refused the whole connection for a startup parameter named "+search_path".
// The URL carried no pool parameter at all, so the function corrupted a URL it
// had nothing to do with.
//
// The rows therefore assert the exact string, not a parsed form: a comparison
// through url.Values would decode both spellings to the same value and see
// nothing.
func TestRemovePostgresPoolParams_KeepsEveryOtherParameterByte(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "a libpq options value survives a URL with no pool parameter",
			url:  "postgres://h:5432/d?sslmode=disable&options=-c%20search_path%3Dextra",
			want: "postgres://h:5432/d?sslmode=disable&options=-c%20search_path%3Dextra",
		},
		{
			name: "and survives the removal of one",
			url:  "postgres://h:5432/d?pool_max_conns=4&options=-c%20search_path%3Dextra",
			want: "postgres://h:5432/d?options=-c%20search_path%3Dextra",
		},
		{
			name: "both pool parameters go",
			url:  "postgres://h:5432/d?pool_max_conns=4&sslmode=disable&pool_min_conns=1",
			want: "postgres://h:5432/d?sslmode=disable",
		},
		{
			// Parameter order is the author's, because nothing here sorts.
			name: "the order the caller wrote is the order that leaves",
			url:  "postgres://h:5432/d?zeta=1&alpha=2&pool_min_conns=1",
			want: "postgres://h:5432/d?zeta=1&alpha=2",
		},
		{
			name: "a URL with no query is untouched",
			url:  "postgres://h:5432/d",
			want: "postgres://h:5432/d",
		},
		{
			name: "a URL that will not parse is returned as it came",
			url:  "://not a url",
			want: "://not a url",
		},
		{
			name: "removing the only parameter leaves no query",
			url:  "postgres://h:5432/d?pool_max_conns=4",
			want: "postgres://h:5432/d",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(removePostgresPoolParams(test.url), qt.Equals, test.want)
		})
	}
}
