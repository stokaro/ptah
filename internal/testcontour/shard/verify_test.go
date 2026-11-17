package shard_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testcontour/shard"
)

const (
	pkgA = "example.com/a"
	pkgB = "example.com/b"
)

// gatePartition is the two-shard partition of four tests by count. Package a
// outweighs half of the contour, so shard 1 runs a:TestA1 and a:TestA3 and
// shard 2 runs a:TestA2 and the whole of package b.
func gatePartition(c *qt.C) shard.Partition {
	c.Helper()
	partition, err := shard.Plan([]shard.Test{
		{Package: pkgA, Name: "TestA1"},
		{Package: pkgA, Name: "TestA2"},
		{Package: pkgA, Name: "TestA3"},
		{Package: pkgB, Name: "TestB1"},
	}, shard.Timings{}, 2)
	c.Assert(err, qt.IsNil)
	return partition
}

func event(action, pkg, test string, elapsed float64) string {
	return fmt.Sprintf(`{"Action":%q,"Package":%q,"Test":%q,"Elapsed":%g}`, action, pkg, test, elapsed)
}

// passed is the event pair go test -json writes for a test that ran and
// passed.
func passed(pkg, test string, elapsed float64) string {
	return event("run", pkg, test, 0) + "\n" + event("pass", pkg, test, elapsed)
}

func packagePassed(pkg string, elapsed float64) string {
	return fmt.Sprintf(`{"Action":"pass","Package":%q,"Elapsed":%g}`, pkg, elapsed)
}

func lines(events ...string) string {
	return strings.Join(events, "\n") + "\n"
}

// cleanShard1 and cleanShard2 are the reports of a contour that ran exactly
// as gatePartition planned it.
var (
	cleanShard1 = lines(
		passed(pkgA, "TestA1", 1.5),
		passed(pkgA, "TestA3", 2),
		packagePassed(pkgA, 4),
	)
	cleanShard2 = lines(
		passed(pkgA, "TestA2", 0.25),
		packagePassed(pkgA, 1),
		passed(pkgB, "TestB1", 3),
		packagePassed(pkgB, 3.5),
	)
)

func reports(partition shard.Partition, events ...string) []shard.Report {
	out := make([]shard.Report, 0, len(events))
	for index, stream := range events {
		out = append(out, shard.Report{
			Manifest: shard.Manifest{Shard: index + 1, Shards: len(partition.Parts), Partition: partition},
			Events:   strings.NewReader(stream),
		})
	}
	return out
}

func TestVerify_HappyPath(t *testing.T) {
	c := qt.New(t)
	partition := gatePartition(c)
	// The shards report in either order; the gate sorts them.
	given := reports(partition, cleanShard1, cleanShard2)
	given[0], given[1] = given[1], given[0]

	result, err := shard.Verify(given, 4)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Shards, qt.Equals, 2)
	c.Assert(result.Listed, qt.Equals, 4)
	c.Assert(result.Ran, qt.Equals, 4)
	c.Assert(result.Passed, qt.Equals, 4)
	c.Assert(result.Digest, qt.Equals, partition.Digest)
	c.Assert(result.Weighting, qt.Equals, "test count")
	c.Assert(result.PerShard, qt.DeepEquals, []shard.ShardResult{
		{Shard: 1, Tests: 2, PlannedMS: 2000, ElapsedMS: 4000},
		{Shard: 2, Tests: 2, PlannedMS: 2000, ElapsedMS: 4500},
	})
}

// TestVerify_HappyPathMeasuresTheNextTimings reads the timings back from the
// reports: one entry per top-level test, and a split package's elapsed time
// summed over the shards that ran it.
func TestVerify_HappyPathMeasuresTheNextTimings(t *testing.T) {
	c := qt.New(t)
	result, err := shard.Verify(reports(gatePartition(c), cleanShard1, cleanShard2), 1)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Timings, qt.DeepEquals, shard.Timings{Packages: map[string]shard.PackageTiming{
		pkgA: {ElapsedMS: 5000, Tests: map[string]int64{"TestA1": 1500, "TestA2": 250, "TestA3": 2000}},
		pkgB: {ElapsedMS: 3500, Tests: map[string]int64{"TestB1": 3000}},
	}})
}

// TestVerify_FailurePathRefusesAContourThatDidNotRunExactlyOnce changes one
// thing about the clean reports per row, and each change is refused by name.
func TestVerify_FailurePathRefusesAContourThatDidNotRunExactlyOnce(t *testing.T) {
	tests := []struct {
		name    string
		shard1  string
		shard2  string
		floor   int
		wantErr string
	}{
		{
			name:    "a test is missing",
			shard1:  lines(passed(pkgA, "TestA1", 1), packagePassed(pkgA, 1)),
			shard2:  cleanShard2,
			floor:   4,
			wantErr: `1 listed tests never ran: example\.com/a:TestA3`,
		},
		{
			name:    "a test ran on two shards",
			shard1:  cleanShard1 + lines(passed(pkgB, "TestB1", 1)),
			shard2:  cleanShard2,
			floor:   4,
			wantErr: "1 tests ran more than once: example\\.com/b:TestB1\n1 tests ran on a shard the partition did not give them: example\\.com/b:TestB1",
		},
		{
			name:    "a test ran twice on its shard",
			shard1:  cleanShard1 + lines(passed(pkgA, "TestA1", 1)),
			shard2:  cleanShard2,
			floor:   4,
			wantErr: `1 tests ran more than once: example\.com/a:TestA1`,
		},
		{
			name:    "a test ran on the other shard",
			shard1:  lines(passed(pkgA, "TestA1", 1), passed(pkgA, "TestA3", 1), passed(pkgB, "TestB1", 1)),
			shard2:  lines(passed(pkgA, "TestA2", 1)),
			floor:   4,
			wantErr: `1 tests ran on a shard the partition did not give them: example\.com/b:TestB1`,
		},
		{
			name:    "a test ran that the contour does not list",
			shard1:  cleanShard1 + lines(passed(pkgA, "TestA4", 1)),
			shard2:  cleanShard2,
			floor:   4,
			wantErr: `1 tests ran that the contour does not list: example\.com/a:TestA4`,
		},
		{
			name:    "a top-level test skipped",
			shard1:  cleanShard1,
			shard2:  lines(passed(pkgA, "TestA2", 1), event("run", pkgB, "TestB1", 0), event("skip", pkgB, "TestB1", 0)),
			floor:   4,
			wantErr: `1 tests or subtests skipped: example\.com/b:TestB1`,
		},
		{
			name:   "a subtest skipped",
			shard1: cleanShard1,
			shard2: cleanShard2 + lines(
				event("run", pkgB, "TestB1/case", 0),
				event("skip", pkgB, "TestB1/case", 0),
			),
			floor:   4,
			wantErr: `1 tests or subtests skipped: example\.com/b:TestB1/case`,
		},
		{
			name:    "a test failed",
			shard1:  cleanShard1,
			shard2:  lines(passed(pkgA, "TestA2", 1), event("run", pkgB, "TestB1", 0), event("fail", pkgB, "TestB1", 1)),
			floor:   4,
			wantErr: `1 tests or packages failed: example\.com/b:TestB1`,
		},
		{
			name:    "a test binary failed outside its tests",
			shard1:  cleanShard1 + lines(event("fail", pkgA, "", 1)),
			shard2:  cleanShard2,
			floor:   4,
			wantErr: `1 tests or packages failed: example\.com/a`,
		},
		{
			name:    "a test never finished",
			shard1:  lines(passed(pkgA, "TestA1", 1), event("run", pkgA, "TestA3", 0), event("fail", pkgA, "", 1)),
			shard2:  cleanShard2,
			floor:   4,
			wantErr: "1 tests started and never finished: example\\.com/a:TestA3\n1 tests or packages failed: example\\.com/a",
		},
		{
			name:    "a shard wrote nothing",
			shard1:  cleanShard1,
			shard2:  "",
			floor:   4,
			wantErr: `2 listed tests never ran: example\.com/a:TestA2, example\.com/b:TestB1`,
		},
		{
			name:    "the listing is below the floor",
			shard1:  cleanShard1,
			shard2:  cleanShard2,
			floor:   5,
			wantErr: `the contour lists 4 tests, below the floor of 5; a listing that stopped matching would look like this`,
		},
		{
			name:    "a report line is not an event",
			shard1:  cleanShard1 + "panic: test timed out\n",
			shard2:  cleanShard2,
			floor:   4,
			wantErr: `shard 1 report line 6 is not a test event: invalid character 'p' looking for beginning of value`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := shard.Verify(reports(gatePartition(c), tc.shard1, tc.shard2), tc.floor)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
		})
	}
}

// TestVerify_FailurePathRefusesReportsThatDisagreeAboutThePartition covers the
// manifests rather than the events: a shard reported twice, or one that carries
// a partition other than its peers', leaves the gate with no agreed contour to
// judge.
func TestVerify_FailurePathRefusesReportsThatDisagreeAboutThePartition(t *testing.T) {
	c := qt.New(t)
	partition := gatePartition(c)
	tampered := partition
	tampered.Tests = tampered.Tests[1:]
	other, err := shard.Plan([]shard.Test{
		{Package: pkgA, Name: "TestA1"},
		{Package: pkgB, Name: "TestB1"},
	}, shard.Timings{}, 2)
	c.Assert(err, qt.IsNil)

	tests := []struct {
		name    string
		reports []shard.Report
		wantErr string
	}{
		{
			name:    "no report",
			wantErr: `no shard reported; nothing proves the contour ran`,
		},
		{
			name: "a shard reported twice",
			reports: append(reports(partition, cleanShard1, cleanShard2), shard.Report{
				Manifest: shard.Manifest{Shard: 2, Shards: 2, Partition: partition},
				Events:   strings.NewReader(cleanShard2),
			}),
			wantErr: `shard 2 reported twice`,
		},
		{
			name: "the shards computed different partitions",
			reports: []shard.Report{
				reports(partition, cleanShard1)[0],
				{Manifest: shard.Manifest{Shard: 2, Shards: 2, Partition: other}, Events: strings.NewReader(cleanShard2)},
			},
			wantErr: `shards 1 and 2 computed different partitions \(sha256:\w+ and sha256:\w+\); their inputs differed`,
		},
		{
			name: "a manifest whose content does not match its digest",
			reports: []shard.Report{
				reports(partition, cleanShard1)[0],
				{Manifest: shard.Manifest{Shard: 2, Shards: 2, Partition: tampered}, Events: strings.NewReader(cleanShard2)},
			},
			wantErr: `shard 2 carries a partition whose digest sha256:\w+ does not match its content \(sha256:\w+\)`,
		},
		{
			name: "the shard counts disagree",
			reports: []shard.Report{
				reports(partition, cleanShard1)[0],
				{Manifest: shard.Manifest{Shard: 2, Shards: 3, Partition: partition}, Events: strings.NewReader(cleanShard2)},
			},
			wantErr: `shard 1 claims 2 shards and shard 2 claims 3`,
		},
		{
			name: "a shard number outside the count",
			reports: []shard.Report{
				reports(partition, cleanShard1)[0],
				{Manifest: shard.Manifest{Shard: 3, Shards: 2, Partition: partition}, Events: strings.NewReader(cleanShard2)},
			},
			wantErr: `a report names shard 3 of 2`,
		},
		{
			name: "a partition of another size",
			reports: []shard.Report{
				{Manifest: shard.Manifest{Shard: 1, Shards: 3, Partition: partition}, Events: strings.NewReader(cleanShard1)},
			},
			wantErr: `shard 1 claims 3 shards but carries a partition of 2 parts`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := shard.Verify(tc.reports, 1)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(result, qt.DeepEquals, shard.Result{})
		})
	}
}

func TestVerify_FailurePathRefusesAFloorThatCannotFail(t *testing.T) {
	c := qt.New(t)
	_, err := shard.Verify(reports(gatePartition(c), cleanShard1, cleanShard2), 0)
	c.Assert(err, qt.ErrorMatches, `the contour floor must be positive, got 0`)
}

// TestVerify_FailurePathKeepsTheAccountBesideTheError shows that a refused
// contour still reports what ran, which is what the gate prints.
func TestVerify_FailurePathKeepsTheAccountBesideTheError(t *testing.T) {
	c := qt.New(t)
	result, err := shard.Verify(reports(gatePartition(c),
		lines(passed(pkgA, "TestA1", 1), packagePassed(pkgA, 1)),
		cleanShard2,
	), 4)
	c.Assert(err, qt.ErrorMatches, `1 listed tests never ran: example\.com/a:TestA3`)
	c.Assert(result.Listed, qt.Equals, 4)
	c.Assert(result.Ran, qt.Equals, 3)
	c.Assert(result.Passed, qt.Equals, 3)
	c.Assert(result.Missing, qt.DeepEquals, []shard.Test{{Package: pkgA, Name: "TestA3"}})
}

// TestVerify_FailurePathJudgesTheShardsThatReported covers a shard that sent
// nothing, as one whose runner lost a service container at startup does. The
// shards that reported still agree on the partition, so the gate refuses the
// contour, counts the absent shard's tests as missing, and still measures the
// rest for the next run's balance.
func TestVerify_FailurePathJudgesTheShardsThatReported(t *testing.T) {
	c := qt.New(t)
	result, err := shard.Verify(reports(gatePartition(c), cleanShard1), 4)
	c.Assert(err, qt.ErrorMatches, `shard 2 of 2 sent no report, so the tests given to it are unaccounted for\n`+
		`2 listed tests never ran: example\.com/a:TestA2, example\.com/b:TestB1`)
	c.Assert(result.Shards, qt.Equals, 2)
	c.Assert(result.Absent, qt.DeepEquals, []int{2})
	c.Assert(result.Ran, qt.Equals, 2)
	c.Assert(result.Passed, qt.Equals, 2)
	c.Assert(result.Missing, qt.DeepEquals, []shard.Test{{Package: pkgA, Name: "TestA2"}, {Package: pkgB, Name: "TestB1"}})
	c.Assert(result.PerShard, qt.HasLen, 1)
	c.Assert(result.Timings.Packages[pkgA].Tests, qt.DeepEquals, map[string]int64{"TestA1": 1500, "TestA3": 2000})
}
