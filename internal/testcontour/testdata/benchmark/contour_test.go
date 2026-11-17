//go:build testcontour_fixture

package benchmark_test

import "testing"

func TestBesideTheBenchmark(_ *testing.T) {}

func BenchmarkNeverRuns(b *testing.B) {
	for b.Loop() {
	}
}
