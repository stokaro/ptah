package ydb

// White-box testing required: the address Open hands ydb-go-sdk is built
// before the SDK dials, and nothing Open returns without a server shows it.
// A parameter Open accepts and then leaves out of that address would be
// accepted and ignored, which is the failure the parameter checks exist to
// prevent.

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydburl"
)

// The parameters the SDK reads reach its address; the ones Ptah reads itself
// do not, so a token never lands in a string the SDK may log.
func TestDataSourceName_PassesWhatTheSDKReads(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{name: "nothing to pass", url: "ydb://localhost/local", want: "grpc://localhost:2136/local"},
		{
			name: "every parameter the SDK reads",
			url: "ydbs://db.example?database=/ru/b1g&go_balancer=disable&go_default_idempotent=true" +
				"&prefetch_query_result_parts=0",
			want: "grpcs://db.example:2135/ru/b1g?go_balancer=disable&go_default_idempotent=true" +
				"&prefetch_query_result_parts=0",
		},
		{name: "the older balancer spelling", url: "ydb://localhost/local?balancer=single",
			want: "grpc://localhost:2136/local?balancer=single"},
		{
			name: "only what Ptah reads itself",
			url:  "ydb://localhost/local?token=t1.abc&go_query_mode=query&query_mode=query&use_env_credentials=false",
			want: "grpc://localhost:2136/local",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parsed, err := ydburl.Parse(test.url)
			c.Assert(err, qt.IsNil)
			passed, err := sdkParameters(parsed.Query)
			c.Assert(err, qt.IsNil)

			got := dataSourceName(parsed, passed)

			c.Assert(got, qt.Equals, test.want)
			_, err = url.Parse(got)
			c.Assert(err, qt.IsNil)
		})
	}
}
