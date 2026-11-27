# Go performance evidence

Load this reference for a reported performance problem or an explicitly scoped
optimization. Keep ordinary correctness changes on the normal validation path.

## Measure the production operation

1. State the affected workload and the metric: CPU time, allocations, retained
   memory, latency, or throughput. Locate the production caller before choosing
   a benchmark. A faster helper may not affect the end-to-end operation.
2. Reuse the nearest benchmark or add a representative one at the owning
   boundary. Keep inputs and observable results realistic. Separate setup from
   the measured operation without removing costs the real caller must pay.
3. Choose a diagnostic for the suspected cost using the
   [Go diagnostics guide](https://go.dev/doc/diagnostics). A CPU profile will not
   explain time waiting for a database. Use allocation, block, mutex, or trace
   evidence when the symptom points there. Collect one expensive profile at a
   time to reduce measurement interference.
4. Compare repeated baseline and changed measurements with the same Go version,
   machine, dependency graph, tags, and workload. Use
   [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat) when available
   to report uncertainty. Preserve raw results outside tracked source files.
   A single noisy run does not establish an improvement.

Run only the selected benchmarks with an explicit time and repetition budget.
Choose their names from the code and confirm they execute. Avoid concurrent
background tests or database jobs that contaminate the comparison. Database
latency claims require the relevant engine and workload, not only a mock.

## Preserve the contract

Check output, error identity, ordering, ownership, and cancellation against the
same regression tests after optimizing. A pool or cache needs a measured
benefit plus explicit invalidation and lifetime rules. Do not add unsafe code,
global state, or a new abstraction based only on allocation counts.

Report the measured scope, variance, and tradeoffs. Distinguish a microbenchmark
gain from application latency or total resource use. Stop optional experiments
when the question is answered; use [validation policy](validation.md) to account
for the remaining required checks.
