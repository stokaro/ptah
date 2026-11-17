package shard_test

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testcontour/shard"
)

const (
	root        = "ptah.run/integration"
	atlascmd    = "ptah.run/integration/atlascmd"
	providercli = "ptah.run/integration/providercli"
	safety      = "ptah.run/integration/safety"
)

// fixtureTests is the listing testdata/timings.json measures: one package
// heavier than an equal share of three parts, and three lighter ones.
func fixtureTests() []shard.Test {
	return []shard.Test{
		{Package: root, Name: "TestR1"},
		{Package: root, Name: "TestR2"},
		{Package: root, Name: "TestR3"},
		{Package: root, Name: "TestR4"},
		{Package: root, Name: "TestR5"},
		{Package: root, Name: "TestR6"},
		{Package: providercli, Name: "TestP1"},
		{Package: providercli, Name: "TestP2"},
		{Package: safety, Name: "TestS1"},
		{Package: atlascmd, Name: "TestA1"},
	}
}

func fixtureTimings(c *qt.C) shard.Timings {
	c.Helper()
	file, err := os.Open("testdata/timings.json")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = file.Close() })
	timings, err := shard.ReadTimings(file)
	c.Assert(err, qt.IsNil)
	return timings
}

// syntheticTests lists packages of uneven sizes: package k holds 3k+1 tests,
// so the heaviest outweighs an equal share of every shard count the
// completeness test asks for.
func syntheticTests() []shard.Test {
	var tests []shard.Test
	for pkg := range 12 {
		for test := range 3*pkg + 1 {
			tests = append(tests, shard.Test{
				Package: fmt.Sprintf("example.com/integration/p%02d", pkg),
				Name:    fmt.Sprintf("Test%03d", test),
			})
		}
	}
	return tests
}

// TestPlan_HappyPathBalancesOnTheFixtureTimings pins the whole partition of the
// fixture over three shards. The root package outweighs a third of the
// contour, so it is split by test name; the other three stay whole. Placing
// the heaviest unit first on the lightest part leaves every part at 115 s.
func TestPlan_HappyPathBalancesOnTheFixtureTimings(t *testing.T) {
	c := qt.New(t)
	partition, err := shard.Plan(fixtureTests(), fixtureTimings(c), 3)
	c.Assert(err, qt.IsNil)

	c.Assert(partition.Parts, qt.DeepEquals, []shard.Part{
		{
			Units: []shard.Unit{
				{Package: root, Tests: []string{"TestR4", "TestR6"}},
				{Package: providercli, Whole: true, Tests: []string{"TestP1", "TestP2"}},
			},
			WeightMS: 115000,
		},
		{
			Units: []shard.Unit{
				{Package: root, Tests: []string{"TestR1", "TestR5"}},
				{Package: safety, Whole: true, Tests: []string{"TestS1"}},
			},
			WeightMS: 115000,
		},
		{
			Units: []shard.Unit{
				{Package: root, Tests: []string{"TestR2", "TestR3"}},
				{Package: atlascmd, Whole: true, Tests: []string{"TestA1"}},
			},
			WeightMS: 115000,
		},
	})
	c.Assert(partition.Weighting, qt.Equals, "timings from fixture run (10 of 10 listed tests measured)")
	c.Assert(partition.Digest, qt.Equals, partition.ComputeDigest())
	c.Assert(partition.Digest, qt.Matches, `sha256:[0-9a-f]{64}`)
}

// TestPlan_HappyPathIsDeterministic plans the same listing given in two orders
// and with a copy of the timings: the answer, digest included, is the same.
func TestPlan_HappyPathIsDeterministic(t *testing.T) {
	c := qt.New(t)
	tests := fixtureTests()
	reversed := slices.Clone(tests)
	slices.Reverse(reversed)

	first, err := shard.Plan(tests, fixtureTimings(c), 3)
	c.Assert(err, qt.IsNil)
	second, err := shard.Plan(reversed, fixtureTimings(c), 3)
	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.DeepEquals, first)
}

// TestPlan_HappyPathCoversEveryTestExactlyOnce partitions a listing of uneven
// packages over every shard count up to twelve, by count and by a measurement
// that makes one package dominate.
func TestPlan_HappyPathCoversEveryTestExactlyOnce(t *testing.T) {
	tests := syntheticTests()
	measured := shard.Timings{Source: "synthetic", Packages: map[string]shard.PackageTiming{
		"example.com/integration/p11": {ElapsedMS: 900000, Tests: map[string]int64{"Test000": 600000}},
	}}
	weightings := []struct {
		name    string
		timings shard.Timings
	}{
		{name: "by count"},
		{name: "by timings", timings: measured},
	}
	for _, weighting := range weightings {
		for shards := 1; shards <= 12; shards++ {
			t.Run(fmt.Sprintf("%s over %d shards", weighting.name, shards), func(t *testing.T) {
				c := qt.New(t)
				partition, err := shard.Plan(tests, weighting.timings, shards)
				c.Assert(err, qt.IsNil)
				c.Assert(partition.Parts, qt.HasLen, shards)

				var union []shard.Test
				for _, part := range partition.Parts {
					c.Assert(part.Tests(), qt.Not(qt.HasLen), 0)
					union = append(union, part.Tests()...)
				}
				slices.SortFunc(union, func(left, right shard.Test) int {
					return strings.Compare(left.String(), right.String())
				})
				c.Assert(union, qt.DeepEquals, partition.Tests)
				c.Assert(partition.Tests, qt.HasLen, len(tests))
			})
		}
	}
}

// TestPlan_HappyPathBalancesByCountWithoutTimings leaves the timings empty.
// Every test weighs the same, the four-test package outweighs half of the
// contour and is split, and each part runs two tests.
func TestPlan_HappyPathBalancesByCountWithoutTimings(t *testing.T) {
	c := qt.New(t)
	tests := []shard.Test{
		{Package: "example.com/a", Name: "TestA1"},
		{Package: "example.com/a", Name: "TestA2"},
		{Package: "example.com/a", Name: "TestA3"},
		{Package: "example.com/b", Name: "TestB1"},
	}
	partition, err := shard.Plan(tests, shard.Timings{}, 2)
	c.Assert(err, qt.IsNil)
	c.Assert(partition.Weighting, qt.Equals, "test count")
	c.Assert(partition.Parts, qt.DeepEquals, []shard.Part{
		{Units: []shard.Unit{{Package: "example.com/a", Tests: []string{"TestA1", "TestA3"}}}, WeightMS: 2000},
		{
			Units: []shard.Unit{
				{Package: "example.com/a", Tests: []string{"TestA2"}},
				{Package: "example.com/b", Whole: true, Tests: []string{"TestB1"}},
			},
			WeightMS: 2000,
		},
	})
}

// TestPlan_HappyPathWeighsAnUnmeasuredTestAtTheMedian adds a test the fixture
// timings never saw. It weighs the median of the listed tests the timings
// measure (30, 40 and 60 s), and its package keeps the 5 s the measured run
// spent outside its tests.
func TestPlan_HappyPathWeighsAnUnmeasuredTestAtTheMedian(t *testing.T) {
	c := qt.New(t)
	tests := []shard.Test{
		{Package: providercli, Name: "TestP1"},
		{Package: providercli, Name: "TestP2"},
		{Package: providercli, Name: "TestPNew"},
		{Package: root, Name: "TestR1"},
	}
	partition, err := shard.Plan(tests, fixtureTimings(c), 1)
	c.Assert(err, qt.IsNil)
	c.Assert(partition.Parts[0].WeightMS, qt.Equals, int64(40000+30000+40000+5000+60000))
	c.Assert(partition.Weighting, qt.Equals, "timings from fixture run (3 of 4 listed tests measured)")
}

// TestPlan_HappyPathNamesWhatBalancedIt reads the weighting a run log
// prints. A timing report that holds no package says why it holds none, and
// one that measures other tests is named as such: both balance by count.
func TestPlan_HappyPathNamesWhatBalancedIt(t *testing.T) {
	tests := []struct {
		name    string
		timings shard.Timings
		want    string
	}{
		{
			name: "no report",
			want: "test count",
		},
		{
			name:    "a report that holds no measurement",
			timings: shard.Timings{Source: "no timings: none of the master runs kept one"},
			want:    "test count (no timings: none of the master runs kept one)",
		},
		{
			name: "a report that measures other tests",
			timings: shard.Timings{Source: "run 7", Packages: map[string]shard.PackageTiming{
				root: {ElapsedMS: 1000, Tests: map[string]int64{"TestGone": 1000}},
			}},
			want: "test count (run 7 measured none of the listed tests)",
		},
		{
			name: "a report without a source",
			timings: shard.Timings{Packages: map[string]shard.PackageTiming{
				root: {ElapsedMS: 1000, Tests: map[string]int64{"TestR1": 1000}},
			}},
			want: "timings from an unnamed report (1 of 10 listed tests measured)",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			partition, err := shard.Plan(fixtureTests(), tc.timings, 2)
			c.Assert(err, qt.IsNil)
			c.Assert(partition.Weighting, qt.Equals, tc.want)
		})
	}
}

func TestPlan_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		tests   []shard.Test
		shards  int
		wantErr string
	}{
		{
			name:    "no shard",
			tests:   fixtureTests(),
			shards:  0,
			wantErr: `shard count must be positive, got 0`,
		},
		{
			name:    "empty listing",
			shards:  2,
			wantErr: `the contour lists no tests`,
		},
		{
			name:    "a test listed twice",
			tests:   []shard.Test{{Package: root, Name: "TestR1"}, {Package: root, Name: "TestR1"}},
			shards:  1,
			wantErr: `test ptah\.run/integration:TestR1 is listed twice`,
		},
		{
			name:    "a test without a package",
			tests:   []shard.Test{{Name: "TestR1"}},
			shards:  1,
			wantErr: `listed test ":TestR1" has an empty package or name`,
		},
		{
			name:    "more shards than units",
			tests:   []shard.Test{{Package: root, Name: "TestR1"}, {Package: safety, Name: "TestS1"}},
			shards:  3,
			wantErr: `3 shards exceed the 2 units the contour can be split into`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			partition, err := shard.Plan(tc.tests, shard.Timings{}, tc.shards)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(partition, qt.DeepEquals, shard.Partition{})
		})
	}
}

func TestPlan_FailurePathReportsAnEmptyListingWithItsSentinel(t *testing.T) {
	c := qt.New(t)
	_, err := shard.Plan(nil, shard.Timings{}, 1)
	c.Assert(err, qt.ErrorIs, shard.ErrNoTests)
}

func TestReadTimings_FailurePathRefusesAnotherShape(t *testing.T) {
	c := qt.New(t)
	timings, err := shard.ReadTimings(strings.NewReader(`{"packages": {"p": {"elapsed": 5}}}`))
	c.Assert(err, qt.ErrorMatches, `decode contour timings: json: unknown field "elapsed"`)
	c.Assert(timings, qt.DeepEquals, shard.Timings{})
}

// TestPartInvocations_HappyPath pins the order a part runs in. Packages go in
// import-path order, as go test ./integration/... runs them, so a whole
// package sorted after a split one runs after it; consecutive whole packages
// share a run; and a split package's tests follow the listing, which is the
// order its test binary runs them in, rather than name order.
func TestPartInvocations_HappyPath(t *testing.T) {
	part := shard.Part{Units: []shard.Unit{
		{Package: root, Tests: []string{"TestA", "TestB", "TestC"}},
		{Package: atlascmd, Whole: true, Tests: []string{"TestX"}},
		{Package: providercli, Whole: true, Tests: []string{"TestY"}},
		{Package: safety, Tests: []string{"TestS1"}},
		{Package: safety + "/z", Whole: true, Tests: []string{"TestZ"}},
	}}
	listed := []shard.Test{
		{Package: root, Name: "TestC"},
		{Package: atlascmd, Name: "TestX"},
		{Package: root, Name: "TestA"},
		{Package: root, Name: "TestOther"},
		{Package: root, Name: "TestB"},
		{Package: providercli, Name: "TestY"},
		{Package: safety, Name: "TestS1"},
		{Package: safety + "/z", Name: "TestZ"},
	}
	tests := []struct {
		name   string
		listed []shard.Test
		limit  int
		want   [][]string
	}{
		{
			name:   "every name of a package fits one pattern",
			listed: listed,
			limit:  1 << 10,
			want: [][]string{
				{"-run", `^(?:TestC|TestA|TestB)$`, root},
				{atlascmd, providercli},
				{"-run", `^(?:TestS1)$`, safety},
				{safety + "/z"},
			},
		},
		{
			name:   "a pattern that holds two names splits in listing order",
			listed: listed,
			limit:  len(`^(?:TestC|TestA)$`),
			want: [][]string{
				{"-run", `^(?:TestC|TestA)$`, root},
				{"-run", `^(?:TestB)$`, root},
				{atlascmd, providercli},
				{"-run", `^(?:TestS1)$`, safety},
				{safety + "/z"},
			},
		},
		{
			name:   "a limit one byte short of two names counts the separator",
			listed: listed,
			limit:  len(`^(?:TestC|TestA)$`) - 1,
			want: [][]string{
				{"-run", `^(?:TestC)$`, root},
				{"-run", `^(?:TestA)$`, root},
				{"-run", `^(?:TestB)$`, root},
				{atlascmd, providercli},
				{"-run", `^(?:TestS1)$`, safety},
				{safety + "/z"},
			},
		},
		{
			name:   "a name longer than the limit keeps a pattern of its own",
			listed: listed,
			limit:  4,
			want: [][]string{
				{"-run", `^(?:TestC)$`, root},
				{"-run", `^(?:TestA)$`, root},
				{"-run", `^(?:TestB)$`, root},
				{atlascmd, providercli},
				{"-run", `^(?:TestS1)$`, safety},
				{safety + "/z"},
			},
		},
		{
			name: "a test the listing does not name runs last, in name order",
			listed: []shard.Test{
				{Package: root, Name: "TestB"},
			},
			limit: 1 << 10,
			want: [][]string{
				{"-run", `^(?:TestB|TestA|TestC)$`, root},
				{atlascmd, providercli},
				{"-run", `^(?:TestS1)$`, safety},
				{safety + "/z"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(part.Invocations(tc.listed, tc.limit), qt.DeepEquals, tc.want)
		})
	}
}

// TestPartInvocations_HappyPathWholePartsShareOneRun covers a part that holds
// no split package: it runs as one go test run with no filter.
func TestPartInvocations_HappyPathWholePartsShareOneRun(t *testing.T) {
	c := qt.New(t)
	part := shard.Part{Units: []shard.Unit{
		{Package: atlascmd, Whole: true, Tests: []string{"TestX"}},
		{Package: safety, Whole: true, Tests: []string{"TestS1"}},
	}}
	c.Assert(part.Invocations(nil, 1<<10), qt.DeepEquals, [][]string{{atlascmd, safety}})
}

// TestPartInvocations_HappyPathSelectsExactlyTheUnit compiles each pattern as
// go test would and checks that the patterns together select the unit's names
// and nothing that only shares a prefix with them.
func TestPartInvocations_HappyPathSelectsExactlyTheUnit(t *testing.T) {
	c := qt.New(t)
	part := shard.Part{Units: []shard.Unit{{Package: root, Tests: []string{"TestApply", "TestApplyDown", "TestPlan"}}}}
	var selected []string
	for _, invocation := range part.Invocations(nil, len(`^(?:TestApply|TestApplyDown)$`)) {
		compiled := regexp.MustCompile(invocation[1])
		for _, name := range []string{"TestApply", "TestApplyDown", "TestApplyDownAll", "TestPlan", "TestPlanX", "XTestPlan"} {
			selected = append(selected, compiled.FindString(name))
		}
	}
	c.Assert(slices.DeleteFunc(selected, func(name string) bool { return name == "" }), qt.DeepEquals,
		[]string{"TestApply", "TestApplyDown", "TestPlan"})
}
