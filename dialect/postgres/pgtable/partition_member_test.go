package pgtable_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
)

const january = "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')"

func member(parent, bound string) pgtable.PartitionMember {
	return pgtable.PartitionMember{Parent: parent, Bound: bound}
}

func TestParsePartitionBound_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, text, want string
	}{
		{name: "range", text: january, want: january},
		{name: "range in lower case", text: "for values from (1) to (2)", want: "for values from (1) to (2)"},
		{name: "range with several columns", text: "FOR VALUES FROM (MINVALUE, 0) TO ('2026-01-01', MAXVALUE)", want: "FOR VALUES FROM (MINVALUE, 0) TO ('2026-01-01', MAXVALUE)"},
		{name: "list", text: "FOR VALUES IN ('a', NULL)", want: "FOR VALUES IN ('a', NULL)"},
		{name: "list with a parenthesis and a comma in a string", text: "FOR VALUES IN ('a)b', 'c,d')", want: "FOR VALUES IN ('a)b', 'c,d')"},
		{name: "hash", text: "FOR VALUES WITH (MODULUS 4, REMAINDER 0)", want: "FOR VALUES WITH (MODULUS 4, REMAINDER 0)"},
		{name: "hash as the catalog prints it", text: "FOR VALUES WITH (modulus 4, remainder 0)", want: "FOR VALUES WITH (modulus 4, remainder 0)"},
		{name: "default", text: "DEFAULT", want: "DEFAULT"},
		{name: "a value computed by a call", text: "FOR VALUES FROM (make_date(2026, 1, 1)) TO (make_date(2027, 1, 1))", want: "FOR VALUES FROM (make_date(2026, 1, 1)) TO (make_date(2027, 1, 1))"},
		{name: "whitespace collapses", text: "FOR  VALUES\n\tIN   ('a')", want: "FOR VALUES IN ('a')"},
		{name: "comments go", text: "FOR VALUES /* c */ IN ('a') -- trailing", want: "FOR VALUES IN ('a')"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgtable.ParsePartitionBound(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestParsePartitionBound_FailurePath(t *testing.T) {
	for _, text := range []string{
		"", "FOR", "FOR VALUES", "FOR VALUES IN", "FOR VALUES IN ()", "FOR VALUES IN ('a'", "FOR VALUES FROM (1)", "FOR VALUES FROM (1) TO", "FOR VALUES FROM (1) TO ()",
		"FOR VALUES BETWEEN 1 AND 2", "DEFAULT extra", "FOR VALUES IN ('a') extra", "FOR VALUES IN ('a'); DROP TABLE t", "default; COMMIT",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgtable.ParsePartitionBound(text)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestSplitPartitionBound_ReturnsWhatFollows(t *testing.T) {
	for _, test := range []struct {
		name, text, bound, rest string
	}{
		{name: "nothing", text: "DEFAULT", bound: "DEFAULT"},
		{name: "a storage clause after default", text: "DEFAULT WITH (fillfactor=70)", bound: "DEFAULT", rest: "WITH (fillfactor=70)"},
		{name: "a hash bound keeps its own WITH", text: "FOR VALUES WITH (MODULUS 2, REMAINDER 0) USING heap", bound: "FOR VALUES WITH (MODULUS 2, REMAINDER 0)", rest: "USING heap"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			bound, rest, err := pgtable.SplitPartitionBound(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(bound, qt.Equals, test.bound)
			c.Assert(rest, qt.Equals, test.rest)
		})
	}
}

// TestPartitionBoundKey_FoldsSpellingAndKeepsValues holds the two directions apart:
// a change of keyword case or whitespace is no difference, and a change of a literal
// is one, so the comparison needs a server to say `'2026-01-01'` and the catalog's
// `'2026-01-01 00:00:00+00'` are the same instant.
func TestPartitionBoundKey_FoldsSpellingAndKeepsValues(t *testing.T) {
	for _, test := range []struct {
		name, a, b string
		same       bool
	}{
		{name: "keyword case", a: "FOR VALUES WITH (MODULUS 2, REMAINDER 0)", b: "FOR VALUES WITH (modulus 2, remainder 0)", same: true},
		{name: "whitespace", a: "FOR VALUES IN ('a','b')", b: "FOR VALUES IN ('a', 'b')", same: true},
		{name: "minvalue", a: "FOR VALUES FROM (minvalue) TO (1)", b: "FOR VALUES FROM (MINVALUE) TO (1)", same: true},
		{name: "another value", a: "FOR VALUES IN ('a')", b: "FOR VALUES IN ('b')"},
		{name: "another case of a string", a: "FOR VALUES IN ('a')", b: "FOR VALUES IN ('A')"},
		{name: "a value in another spelling", a: "FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')", b: "FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')"},
		{name: "another bound form", a: "DEFAULT", b: "FOR VALUES IN ('a')"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtable.PartitionBoundKey(test.a) == pgtable.PartitionBoundKey(test.b), qt.Equals, test.same)
		})
	}
}

func TestValidatePartitionMember_FailurePath(t *testing.T) {
	key := pgtable.PartitionBoundKey(january)
	for _, test := range []struct {
		name  string
		value *pgtable.DesiredPartitionMember
	}{
		{name: "nil"},
		{name: "no parent", value: &pgtable.DesiredPartitionMember{PartitionMember: member("", january)}},
		{name: "no bound", value: &pgtable.DesiredPartitionMember{PartitionMember: member("events", "")}},
		{name: "a statement in the bound", value: &pgtable.DesiredPartitionMember{PartitionMember: member("events", "DEFAULT; DROP TABLE events")}},
		{name: "a fact about another bound", value: &pgtable.DesiredPartitionMember{PartitionMember: member("events", "DEFAULT"),
			Normalized: &pgtable.NormalizedPartitionBound{SourceKey: key, Key: key}}},
		{name: "a fact with no key", value: &pgtable.DesiredPartitionMember{PartitionMember: member("events", january),
			Normalized: &pgtable.NormalizedPartitionBound{SourceKey: key}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtable.ValidateDesiredPartitionMember(test.value), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func TestPartitionMemberCodecs_RoundTripBothRepresentations(t *testing.T) {
	codecs := pgtable.PartitionMemberCodecs()
	for _, test := range []struct {
		name  string
		codec schemaext.Codec
		value schemaext.Payload
	}{
		{name: "declared", codec: codecs[0], value: &pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{ParentSchema: "app", Parent: "events", Bound: january}}},
		{name: "observed", codec: codecs[1], value: &pgtable.ObservedPartitionMember{Parent: "events", Bound: "DEFAULT"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := test.codec.Encode(test.value)
			c.Assert(err, qt.IsNil)
			decoded, err := test.codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, test.value)
		})
	}
}

func TestPartitionMemberCodecs_RejectMalformedValues(t *testing.T) {
	for _, codec := range pgtable.PartitionMemberCodecs() {
		for _, raw := range []string{
			`null`, `{}`, `{"parent":"events"}`, `{"bound":"DEFAULT"}`, `{"parent":"","bound":"DEFAULT"}`, `{"parent":"events","bound":"OTHER"}`,
			`{"parent":"events","bound":"DEFAULT","extra":true}`, `{"parent":"events","parent":"other","bound":"DEFAULT"}`,
			`{"parent":"events","bound":"DEFAULT; COMMIT"}`,
		} {
			t.Run(raw, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(raw))
				c.Assert(err, qt.IsNotNil)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}

// TestPartitionMember_DefinesItsTableOnlyAsADeclaration pins the two answers the
// comparison reads. A declaration has no column list. An observation lists its
// columns, and a source with no syntax for a partition still plans its removal.
func TestPartitionMember_DefinesItsTableOnlyAsADeclaration(t *testing.T) {
	c := qt.New(t)
	desired := must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{PartitionMember: member("events", january)}))
	observed := must.Must(schemaext.NewFacets(&pgtable.ObservedPartitionMember{Parent: "events", Bound: january}))

	_, declaredDefines := schemaext.DefiningKind(desired)
	_, observedDefines := schemaext.DefiningKind(observed)

	c.Assert(declaredDefines, qt.IsTrue)
	c.Assert(observedDefines, qt.IsFalse)
	for _, facets := range []schemaext.Facets{desired, observed} {
		schema, name, found := schemaext.ParentTableOf(facets)
		c.Assert(found, qt.IsTrue)
		c.Assert(schema, qt.Equals, "")
		c.Assert(name, qt.Equals, "events")
	}
}

func TestPartitionMemberConversion_DropsProbeFacts(t *testing.T) {
	c := qt.New(t)
	key := pgtable.PartitionBoundKey(january)
	desired := &pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{ParentSchema: "app", Parent: "events", Bound: january},
		Normalized: &pgtable.NormalizedPartitionBound{SourceKey: key, Key: key}}
	service := pgtable.PartitionMemberService{}

	observed, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Desired, To: schemaext.Observed, Values: []schemaext.Value{desired}})
	c.Assert(err, qt.IsNil)
	returned, err := service.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "postgres", From: schemaext.Observed, To: schemaext.Desired, Values: observed})

	c.Assert(err, qt.IsNil)
	c.Assert(observed[0], qt.DeepEquals, schemaext.Value(&pgtable.ObservedPartitionMember{ParentSchema: "app", Parent: "events", Bound: january}))
	c.Assert(returned[0], qt.DeepEquals, schemaext.Value(&pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{ParentSchema: "app", Parent: "events", Bound: january}}))
}

func memberSubject(name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", name)
}

// memberRequest compares table e1 alone, with each side's value and coverage as the
// row gives them. A nil value leaves that side without a facet.
func memberRequest(desired *pgtable.DesiredPartitionMember, current *pgtable.ObservedPartitionMember, desiredCoverage, currentCoverage schemaext.Coverage) schemaext.FacetComparisonRequest {
	subject := memberSubject("e1")
	request := schemaext.FacetComparisonRequest{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Kinds: []schemaext.Kind{pgtable.PartitionMemberKind},
		Owners: []schemaext.ParentState{{Subject: subject, Desired: true, Current: true}, {Subject: memberSubject("events"), Desired: true, Current: true}}}
	if desired != nil {
		request.Desired.Records = []schemaext.FacetRecord{{Subject: subject, Values: must.Must(schemaext.NewFacets(desired))}}
	}
	if current != nil {
		request.Current.Records = []schemaext.FacetRecord{{Subject: subject, Values: must.Must(schemaext.NewFacets(current))}}
	}
	request.Desired.Coverage, request.Current.Coverage = desiredCoverage, currentCoverage
	return request
}

func declaredMembers() schemaext.Coverage {
	return must.Must(pgtable.PartitionMemberCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
}

func observedMembers() schemaext.Coverage {
	return must.Must(pgtable.PartitionMemberCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
}

func TestPartitionMemberComparison_HappyPath(t *testing.T) {
	server := "FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2027-01-01 00:00:00+00')"
	normalized := &pgtable.NormalizedPartitionBound{SourceKey: pgtable.PartitionBoundKey(january), Key: pgtable.PartitionBoundKey(server)}
	for _, test := range []struct {
		name                             string
		desired                          *pgtable.DesiredPartitionMember
		current                          *pgtable.ObservedPartitionMember
		desiredCoverage, currentCoverage schemaext.Coverage
		undecided                        int
	}{
		{name: "the same bound as written", desired: &pgtable.DesiredPartitionMember{PartitionMember: member("events", january)},
			current: &pgtable.ObservedPartitionMember{Parent: "events", Bound: january}, desiredCoverage: declaredMembers(), currentCoverage: observedMembers()},
		{name: "a server's spelling of the bound", desired: &pgtable.DesiredPartitionMember{PartitionMember: member("events", january), Normalized: normalized},
			current: &pgtable.ObservedPartitionMember{Parent: "events", Bound: server}, desiredCoverage: declaredMembers(), currentCoverage: observedMembers()},
		{name: "a parent spelled with the default schema", desired: &pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{ParentSchema: "public", Parent: "events", Bound: january}},
			current: &pgtable.ObservedPartitionMember{Parent: "events", Bound: january}, desiredCoverage: declaredMembers(), currentCoverage: observedMembers()},
		{name: "another spelling with no server to ask", desired: &pgtable.DesiredPartitionMember{PartitionMember: member("events", january)},
			current: &pgtable.ObservedPartitionMember{Parent: "events", Bound: server}, desiredCoverage: declaredMembers(), currentCoverage: observedMembers(), undecided: 1},
		{name: "a source with no syntax for a partition asks nothing", current: &pgtable.ObservedPartitionMember{Parent: "events", Bound: january}, currentCoverage: observedMembers()},
		{name: "a current side that does not say", desired: &pgtable.DesiredPartitionMember{PartitionMember: member("events", january)}, desiredCoverage: declaredMembers(), undecided: 1},
		{name: "an ordinary table on both sides", desiredCoverage: declaredMembers(), currentCoverage: observedMembers()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := memberRequest(test.desired, test.current, test.desiredCoverage, test.currentCoverage)

			result, err := (pgtable.PartitionMemberService{}).CompareFacets(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
		})
	}
}

func TestPartitionMemberComparison_FailurePath(t *testing.T) {
	other := "FOR VALUES FROM ('2028-01-01') TO ('2029-01-01')"
	for _, test := range []struct {
		name    string
		desired *pgtable.DesiredPartitionMember
		current *pgtable.ObservedPartitionMember
		want    string
	}{
		{name: "another bound the server normalized", desired: &pgtable.DesiredPartitionMember{PartitionMember: member("events", other),
			Normalized: &pgtable.NormalizedPartitionBound{SourceKey: pgtable.PartitionBoundKey(other), Key: pgtable.PartitionBoundKey(other)}},
			current: &pgtable.ObservedPartitionMember{Parent: "events", Bound: january},
			want:    `.*table public\.e1 is a partition with bound FOR VALUES FROM \('2026-01-01'\) TO \('2027-01-01'\), and the declaration gives it FOR VALUES FROM \('2028-01-01'\) TO \('2029-01-01'\); a reviewed manual partition cutover is required`},
		{name: "another parent", desired: &pgtable.DesiredPartitionMember{PartitionMember: member("other", january)},
			current: &pgtable.ObservedPartitionMember{Parent: "events", Bound: january},
			want:    `.*table public\.e1 is a partition of events, and the declaration makes it a partition of other; a reviewed manual partition cutover is required`},
		{name: "a partition declared as an ordinary table", current: &pgtable.ObservedPartitionMember{Parent: "events", Bound: january},
			want: `.*table public\.e1 is a partition of events, and the declaration makes it an ordinary table; a reviewed manual partition cutover is required`},
		{name: "an ordinary table declared as a partition", desired: &pgtable.DesiredPartitionMember{PartitionMember: member("events", january)},
			want: `.*table public\.e1 is an ordinary table, and the declaration makes it a partition of events; a reviewed manual partition cutover is required`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := (pgtable.PartitionMemberService{}).CompareFacets(t.Context(), memberRequest(test.desired, test.current, declaredMembers(), observedMembers()))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
		})
	}
}

// TestPartitionMemberComparison_RefusesARemovedParent reads what the plan would do:
// a parent the database holds and the document leaves out is dropped, and a DROP of
// a partitioned table takes the partition the document keeps.
func TestPartitionMemberComparison_RefusesARemovedParent(t *testing.T) {
	c := qt.New(t)
	request := memberRequest(&pgtable.DesiredPartitionMember{PartitionMember: member("events", january)}, &pgtable.ObservedPartitionMember{Parent: "events", Bound: january}, declaredMembers(), observedMembers())
	request.Owners[1].Desired = false

	result, err := (pgtable.PartitionMemberService{}).CompareFacets(t.Context(), request)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*table public\.e1 is declared a partition of public\.events, which the database holds and the desired schema does not declare; dropping a partitioned table drops its partitions`)
	c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
}

// TestPartitionMemberComparison_LeavesAParentOutsideTheRead is the control: a parent
// neither side lists, as in a schema the run does not read, is the server's to
// resolve.
func TestPartitionMemberComparison_LeavesAParentOutsideTheRead(t *testing.T) {
	c := qt.New(t)
	request := memberRequest(&pgtable.DesiredPartitionMember{PartitionMember: member("events", january)}, &pgtable.ObservedPartitionMember{Parent: "events", Bound: january}, declaredMembers(), observedMembers())
	request.Owners = request.Owners[:1]

	result, err := (pgtable.PartitionMemberService{}).CompareFacets(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Undecided, qt.HasLen, 0)
}

func TestPartitionMemberComparison_RefusesAForeignTarget(t *testing.T) {
	for _, target := range []string{"mysql", "cockroachdb", "spanner", "sqlite"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			request := memberRequest(&pgtable.DesiredPartitionMember{PartitionMember: member("events", january)}, nil, declaredMembers(), observedMembers())
			request.Target = target

			_, err := (pgtable.PartitionMemberService{}).CompareFacets(t.Context(), request)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*CREATE TABLE \.\.\. PARTITION OF is a PostgreSQL partition declaration, which `+target+` has no statement for`)
		})
	}
}

func memberTable(action featureplan.ParentAction) featureplan.Table {
	declared := schemamodel.Table{StructName: "E1", Name: "e1", Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{PartitionMember: member("events", january)}))}
	observed := catalog.Table{Name: "e1", Facets: must.Must(schemaext.NewFacets(&pgtable.ObservedPartitionMember{Parent: "events", Bound: january}))}
	return featureplan.Table{Subject: memberSubject("e1"), Action: action, Desired: schemacapture.TableDeclaration{Table: declared}, Current: schemacapture.TableObservation{Table: observed}}
}

func TestPartitionMemberPlanning_HappyPath(t *testing.T) {
	for _, test := range []struct {
		action   featureplan.ParentAction
		strategy string
	}{
		{action: featureplan.CreateTable, strategy: "CREATE TABLE carries the declared parent and bound"},
		{action: featureplan.DropTable, strategy: "remove the partition and its rows with the table"},
		{action: featureplan.AlterTable, strategy: "retain the table's parent and bound"},
	} {
		t.Run(string(test.action), func(t *testing.T) {
			c := qt.New(t)

			result, err := (pgtable.PartitionMemberService{}).PlanFeatures(t.Context(), featureplan.Request{Target: "postgres",
				ParentKinds: []schemaext.Kind{pgtable.PartitionMemberKind}, Tables: []featureplan.Table{memberTable(test.action)}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Parents, qt.HasLen, 1)
			c.Assert(result.Parents[0].Strategy, qt.Equals, test.strategy)
		})
	}
}

func TestPartitionMemberPlanning_RefusesARebuild(t *testing.T) {
	c := qt.New(t)

	_, err := (pgtable.PartitionMemberService{}).PlanFeatures(t.Context(), featureplan.Request{Target: "postgres",
		ParentKinds: []schemaext.Kind{pgtable.PartitionMemberKind}, Tables: []featureplan.Table{memberTable(featureplan.RebuildTable)}})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*a PostgreSQL partition has no reconstruction plan for parent action "rebuild-table"`)
}

func TestPartitionReports_CountEachRepresentation(t *testing.T) {
	for _, test := range []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
		service        schemaext.ReportingService
		metric         string
	}{
		{name: "a declared key", representation: schemaext.Desired, value: &pgtable.DesiredPartition{Type: "LIST", Parts: []pgtable.PartitionPart{{Name: "a"}}},
			service: pgtable.PartitionService{}, metric: "postgres_partitioned_tables"},
		{name: "an observed key", representation: schemaext.Observed, value: &pgtable.ObservedPartition{Type: "LIST", Parts: []pgtable.PartitionPart{{Name: "a"}}},
			service: pgtable.PartitionService{}, metric: "postgres_partitioned_tables"},
		{name: "a declared partition", representation: schemaext.Desired, value: &pgtable.DesiredPartitionMember{PartitionMember: member("events", january)},
			service: pgtable.PartitionMemberService{}, metric: "postgres_table_partitions"},
		{name: "an observed partition", representation: schemaext.Observed, value: &pgtable.ObservedPartitionMember{Parent: "events", Bound: january},
			service: pgtable.PartitionMemberService{}, metric: "postgres_table_partitions"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reports, err := test.service.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(reports, qt.HasLen, 1)
			c.Assert(reports[0].Counts, qt.DeepEquals, []schemaext.MetricCount{{Name: test.metric, Value: 1}})
		})
	}
}
