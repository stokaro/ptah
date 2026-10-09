package crdbschema_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/cockroachdb/crdbschema"
)

// fullPolicy sets every managed parameter.
func fullPolicy() crdbschema.Policy {
	return crdbschema.Policy{
		ExpirationExpression: "expires_at + INTERVAL '1 day'", ExpireAfter: "3 days", RowStatsPollInterval: "10m",
		JobCron: "@daily", SelectBatchSize: new(int64(500)), DeleteBatchSize: new(int64(100)),
		SelectRateLimit: new(int64(200)), DeleteRateLimit: new(int64(300)),
		Pause: true, LabelMetrics: true, DisableChangefeedReplication: true,
	}
}

// TestParameters_FollowTheStatementOrder pins the order a statement names the
// parameters. A plan is fingerprinted and re-approved by a person, so a
// statement whose parameter order varied between runs over the same two
// states would churn both.
func TestParameters_FollowTheStatementOrder(t *testing.T) {
	c := qt.New(t)

	var names []string
	for _, parameter := range fullPolicy().Parameters() {
		names = append(names, parameter.Name)
	}

	c.Assert(names, qt.DeepEquals, crdbschema.ManagedParameters())
	c.Assert(names, qt.DeepEquals, []string{
		"ttl_expiration_expression", "ttl_expire_after", "ttl_row_stats_poll_interval", "ttl_job_cron",
		"ttl_select_batch_size", "ttl_delete_batch_size", "ttl_select_rate_limit", "ttl_delete_rate_limit",
		"ttl_pause", "ttl_label_metrics", "ttl_disable_changefeed_replication",
	})
	c.Assert(crdbschema.Policy{}.Parameters(), qt.IsNil)
	c.Assert(crdbschema.Policy{ExpirationExpression: "expires_at", Pause: false}.Parameters(), qt.DeepEquals,
		[]crdbschema.Parameter{{Name: "ttl_expiration_expression", Value: "expires_at", Kind: crdbschema.TextValue}})
}

// TestEveryPolicyFieldIsAParameter derives the census from the struct, so a
// field added to Policy must join the parameter vocabulary that rendering,
// reading, encoding and comparison share.
func TestEveryPolicyFieldIsAParameter(t *testing.T) {
	c := qt.New(t)

	c.Assert(reflect.TypeFor[crdbschema.Policy]().NumField(), qt.Equals, len(fullPolicy().Parameters()))
}

// TestPolicy_CloneSharesNoCount pins that a clone cannot reach back into the
// policy it came from through a count pointer.
func TestPolicy_CloneSharesNoCount(t *testing.T) {
	c := qt.New(t)

	original := fullPolicy()
	cloned := original.Clone()
	*cloned.SelectBatchSize = 1
	*cloned.DeleteRateLimit = 1

	c.Assert(*original.SelectBatchSize, qt.Equals, int64(500))
	c.Assert(*original.DeleteRateLimit, qt.Equals, int64(300))
	c.Assert(cloned.Same(original), qt.IsFalse)
}

// TestPolicy_SameIsStructural pins that structural equality does no reading of
// its own: two spellings of one interval are different values here. The
// comparison owner reads them as values.
func TestPolicy_SameIsStructural(t *testing.T) {
	tests := []struct {
		name  string
		a, b  crdbschema.Policy
		equal bool
	}{
		{name: "identical", a: fullPolicy(), b: fullPolicy(), equal: true},
		{name: "two spellings of one interval", a: crdbschema.Policy{ExpireAfter: "72 hours"}, b: crdbschema.Policy{ExpireAfter: "72:00:00"}},
		{name: "a count on one side", a: crdbschema.Policy{SelectBatchSize: new(int64(1))}, b: crdbschema.Policy{}},
		{name: "different counts", a: crdbschema.Policy{SelectBatchSize: new(int64(1))}, b: crdbschema.Policy{SelectBatchSize: new(int64(2))}},
		{name: "a flag on one side", a: crdbschema.Policy{LabelMetrics: true}, b: crdbschema.Policy{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.a.Same(test.b), qt.Equals, test.equal)
			c.Assert(test.b.Same(test.a), qt.Equals, test.equal)
		})
	}
}

// TestRowTTL_RepresentationsAndCodecsRoundTrip pins both representations
// through their codecs, and the projection between them.
func TestRowTTL_RepresentationsAndCodecsRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		policy crdbschema.Policy
	}{
		{name: "every parameter", policy: fullPolicy()},
		{name: "the expression enabler alone", policy: crdbschema.Policy{ExpirationExpression: "expires_at"}},
		{name: "the interval enabler alone", policy: crdbschema.Policy{ExpireAfter: "72 hours"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &crdbschema.DesiredRowTTL{Policy: test.policy}
			observed, err := desired.Observed()
			c.Assert(err, qt.IsNil)
			c.Assert(observed.Policy, qt.DeepEquals, test.policy)
			c.Assert(observed.Desired().Equal(desired), qt.IsTrue)
			for index, value := range []schemaext.Value{desired, observed} {
				codec := crdbschema.Codecs()[index]
				encoded, err := codec.Encode(value)
				c.Assert(err, qt.IsNil)
				decoded, err := codec.Decode(encoded)
				c.Assert(err, qt.IsNil)
				c.Assert(value.Equal(decoded.(schemaext.Value)), qt.IsTrue)
				canonical, err := codec.Canonical(value)
				c.Assert(err, qt.IsNil)
				c.Assert(canonical, qt.DeepEquals, encoded)
			}
		})
	}
}

// TestRowTTL_WireSpellsEachParameterByItsName pins the wire object: the
// storage parameter names, a count as an integer and a flag as true.
func TestRowTTL_WireSpellsEachParameterByItsName(t *testing.T) {
	c := qt.New(t)

	encoded, err := crdbschema.Codecs()[0].Encode(&crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{
		ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500)), Pause: true,
	}})

	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"ttl_expiration_expression":"expires_at","ttl_pause":true,"ttl_select_batch_size":500}`)
}

// TestRowTTL_CodecsRefuseWhatHasNoCanonicalSpelling pins the decode refusals.
func TestRowTTL_CodecsRefuseWhatHasNoCanonicalSpelling(t *testing.T) {
	tests := []struct {
		name           string
		representation int
		data           string
	}{
		{name: "null", data: `null`},
		{name: "an unknown property", data: `{"ttl_expiration_expression":"a","ttl":"on"}`},
		{name: "a property spelled in another case", data: `{"TTL_EXPIRATION_EXPRESSION":"a"}`},
		{name: "a null property", data: `{"ttl_expiration_expression":"a","ttl_job_cron":null}`},
		{name: "an empty text value", data: `{"ttl_expiration_expression":"a","ttl_job_cron":""}`},
		{name: "a false flag", data: `{"ttl_expiration_expression":"a","ttl_pause":false}`},
		{name: "a text count", data: `{"ttl_expiration_expression":"a","ttl_select_batch_size":"500"}`},
		{name: "a fractional count", data: `{"ttl_expiration_expression":"a","ttl_select_batch_size":1.5}`},
		{name: "a declaration without an expiry", data: `{"ttl_job_cron":"@daily"}`},
		{name: "a declaration with a zero count", data: `{"ttl_expiration_expression":"a","ttl_select_batch_size":0}`},
		{name: "an observation without an expiry", representation: 1, data: `{"ttl_job_cron":"@daily"}`},
		{name: "an empty observation", representation: 1, data: `{}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			decoded, err := crdbschema.Codecs()[test.representation].Decode([]byte(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

func TestValidateDesired_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		policy crdbschema.Policy
	}{
		{name: "the expression enabler", policy: crdbschema.Policy{ExpirationExpression: "expires_at"}},
		{name: "the interval enabler", policy: crdbschema.Policy{ExpireAfter: "3 days"}},
		{name: "an interval the server will respell", policy: crdbschema.Policy{ExpireAfter: "72 hours"}},
		{name: "both enablers, which the server accepts together", policy: crdbschema.Policy{ExpirationExpression: "expires_at", ExpireAfter: "3 days"}},
		{name: "every count at its least value", policy: crdbschema.Policy{
			ExpirationExpression: "expires_at", SelectBatchSize: new(int64(1)), DeleteBatchSize: new(int64(1)),
			SelectRateLimit: new(int64(1)), DeleteRateLimit: new(int64(1)),
		}},
		{name: "every parameter", policy: fullPolicy()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(crdbschema.ValidateDesired(&crdbschema.DesiredRowTTL{Policy: test.policy}), qt.IsNil)
		})
	}
}

func TestValidateDesired_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		value   *crdbschema.DesiredRowTTL
		wantErr string
	}{
		{name: "nil", value: nil, wantErr: `(?s).*nil CockroachDB row-level TTL declaration.*`},
		{
			// Every other ttl_ parameter is refused by the server when no
			// expiry is configured, so Ptah refuses it before the statement.
			name: "a knob with no enabler", value: &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{JobCron: "@daily"}},
			wantErr: `(?s).*name neither ttl_expiration_expression nor ttl_expire_after.*must be set.*`,
		},
		{
			// Zero is the silent shape: accepted, then stored nowhere.
			name:    "a zero count",
			value:   &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(0))}},
			wantErr: `(?s).*ttl_select_batch_size = 0: CockroachDB refuses a negative value.*stores nothing at all for zero.*`,
		},
		{
			name:    "a negative count",
			value:   &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at", DeleteRateLimit: new(int64(-1))}},
			wantErr: `(?s).*ttl_delete_rate_limit = -1.*`,
		},
		{
			// The value is refused, not the parameter: an interval Ptah cannot
			// read would be sent, respelled by the server, and re-issued.
			name:    "an interval this owner cannot read",
			value:   &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "3 fortnights"}},
			wantErr: `(?s).*ttl_expire_after: .*"3 fortnights".*`,
		},
		{
			// Measured: the server truncates to whole seconds and stores
			// nothing when that leaves zero.
			name:    "a poll interval the server would truncate to nothing",
			value:   &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "1 hour", RowStatsPollInterval: "500ms"}},
			wantErr: `(?s).*ttl_row_stats_poll_interval:.*below one second.*`,
		},
		{
			name:    "a poll interval past the largest duration the server holds",
			value:   &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpireAfter: "1 hour", RowStatsPollInterval: "2562048h"}},
			wantErr: `(?s).*ttl_row_stats_poll_interval:.*is longer than.*`,
		},
		{
			name:    "a blank expression",
			value:   &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "  "}},
			wantErr: `(?s).*ttl_expiration_expression cannot contain only whitespace.*`,
		},
		{
			name:    "a NUL byte",
			value:   &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "expires_at\x00"}},
			wantErr: `(?s).*ttl_expiration_expression contains a NUL byte.*`,
		},
		{
			name:    "invalid UTF-8",
			value:   &crdbschema.DesiredRowTTL{Policy: crdbschema.Policy{ExpirationExpression: "\xff"}},
			wantErr: `(?s).*ttl_expiration_expression is not valid UTF-8.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := crdbschema.ValidateDesired(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var invalid *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &invalid)
			c.Assert(invalid.Kind, qt.Equals, crdbschema.RowTTLKind)
			c.Assert(invalid.Representation, qt.Equals, schemaext.Desired)
		})
	}
}

// TestValidateObserved_AcceptsWhatTheServerStores pins that an observation is
// not held to the declaration's reading of intervals: the server's own
// spelling is valid, and the comparison owner reads it.
func TestValidateObserved_AcceptsWhatTheServerStores(t *testing.T) {
	c := qt.New(t)

	c.Assert(crdbschema.ValidateObserved(&crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{
		ExpireAfter: "1 year 2 mons 3 days", RowStatsPollInterval: "10m0s",
	}}), qt.IsNil)
	err := crdbschema.ValidateObserved(nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	err = crdbschema.ValidateObserved(&crdbschema.ObservedRowTTL{Policy: crdbschema.Policy{JobCron: "@daily"}})
	c.Assert(err, qt.ErrorMatches, `(?s).*observed row-level TTL names neither.*`)
}

// TestRowTTLCoverage_RecordsTheClaimItIsGiven pins the source and read claims
// the helper builds, under the owner identity the bundled runtime uses.
func TestRowTTLCoverage_RecordsTheClaimItIsGiven(t *testing.T) {
	c := qt.New(t)

	coverage, err := crdbschema.RowTTLCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Representation(), qt.Equals, schemaext.Desired)
	records := coverage.KindRecords()
	c.Assert(records, qt.HasLen, 1)
	c.Assert(records[0].Model.Owner, qt.Equals, crdbschema.Owner)
	c.Assert(records[0].Model.Kind, qt.Equals, crdbschema.RowTTLKind)
	c.Assert(records[0].Knowledge.State, qt.Equals, schemaext.Complete)
	_, err = crdbschema.RowTTLCoverage(schemaext.Change, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}

// TestRowTTL_ValuesStayIndependentThroughFacets pins that a value read back out
// of a facet collection is a copy.
func TestRowTTL_ValuesStayIndependentThroughFacets(t *testing.T) {
	c := qt.New(t)

	facets := must.Must(schemaext.NewFacets(&crdbschema.DesiredRowTTL{Policy: fullPolicy()}))
	value, found, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](facets, crdbschema.RowTTLKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	*value.Policy.SelectBatchSize = 1

	again, _, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](facets, crdbschema.RowTTLKind)
	c.Assert(err, qt.IsNil)
	c.Assert(*again.Policy.SelectBatchSize, qt.Equals, int64(500))
}
