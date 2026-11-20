package atlashcl

import (
	"fmt"
	"net/url"

	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// The URL functions build a database URL out of parts a project cannot write
// literally: a password that holds `@` or `/`, an Entra user name with `@` in
// it, a query parameter taken from a variable. Their results were read on the
// pinned community binary v1.4.0, and each one is what Go's net/url writes, so
// the four are implemented with that package rather than by hand: the bytes a
// URL serializes to, a percent sign or a sorted query included, are the same.
//
// One behavior is deliberately not reproduced. That binary's urlqueryset
// drops a query pair it cannot decode, so `?%zz=1` becomes `?a=b` without a
// word; here the call fails and names the query, because the dropped pair is
// part of the URL the author wrote.

// urlEscapeFunc escapes a string for use inside a URL, as a query value or a
// userinfo part: `a b@c/d` becomes `a+b%40c%2Fd`.
var urlEscapeFunc = function.New(&function.Spec{
	Params: []function.Parameter{{Name: "s", Type: cty.String}},
	Type:   function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		return cty.StringVal(url.QueryEscape(args[0].AsString())), nil
	},
})

// urlUserInfoFunc replaces a URL's user and password. Without a password, or
// with a null one, the URL carries the user alone and any password it held is
// gone; an empty password is kept as an empty one.
var urlUserInfoFunc = function.New(&function.Spec{
	Params: []function.Parameter{
		{Name: "url", Type: cty.String},
		{Name: "user", Type: cty.String},
	},
	VarParam: &function.Parameter{Name: "pass", Type: cty.String, AllowNull: true},
	Type:     function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		if len(args) > 3 {
			return cty.NilVal, fmt.Errorf("urluserinfo takes a URL, a user and at most one password, got %d arguments", len(args))
		}
		parsed, err := url.Parse(args[0].AsString())
		if err != nil {
			return cty.NilVal, err
		}
		user := args[1].AsString()
		parsed.User = url.User(user)
		if len(args) == 3 && !args[2].IsNull() {
			parsed.User = url.UserPassword(user, args[2].AsString())
		}
		return cty.StringVal(parsed.String()), nil
	},
})

// urlQuerySetFunc sets one query parameter, replacing every value it had, and
// writes the query back sorted by key.
var urlQuerySetFunc = function.New(&function.Spec{
	Params: []function.Parameter{
		{Name: "url", Type: cty.String},
		{Name: "key", Type: cty.String},
		{Name: "value", Type: cty.String},
	},
	Type: function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		parsed, err := url.Parse(args[0].AsString())
		if err != nil {
			return cty.NilVal, err
		}
		query, err := url.ParseQuery(parsed.RawQuery)
		if err != nil {
			return cty.NilVal, fmt.Errorf("the query of %q does not decode, so setting %q would drop part of it: %w",
				args[0].AsString(), args[1].AsString(), err)
		}
		query.Set(args[1].AsString(), args[2].AsString())
		parsed.RawQuery = query.Encode()
		return cty.StringVal(parsed.String()), nil
	},
})

// urlSetPathFunc replaces a URL's path, the database name in most database
// URLs. A path without a leading slash gets one, and an empty path leaves the
// URL with none.
var urlSetPathFunc = function.New(&function.Spec{
	Params: []function.Parameter{
		{Name: "url", Type: cty.String},
		{Name: "path", Type: cty.String},
	},
	Type: function.StaticReturnType(cty.String),
	Impl: func(args []cty.Value, _ cty.Type) (cty.Value, error) {
		parsed, err := url.Parse(args[0].AsString())
		if err != nil {
			return cty.NilVal, err
		}
		parsed.Path = args[1].AsString()
		parsed.RawPath = ""
		return cty.StringVal(parsed.String()), nil
	},
})
