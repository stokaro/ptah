# Contributing to Ptah

Thanks for taking the time. This page covers filing an issue, proposing a
change, and the checks a change has to pass.

[AGENTS.md](AGENTS.md) is the authority on how work is done in this repository.
It is written for coding agents, and it is equally the rulebook for people:
where work happens, what each gate measures, and the decisions that have to be
right before a check could run. This page does not restate it, because a second
copy drifts the moment the first one moves.

## Before you open an issue

Search the existing issues first. Ptah is pre-GA and moves quickly, so a
surprising behavior is often already recorded, sometimes with the measurement
that explains it.

Say which binary you ran. `ptah` and `ptah-compat` present different command
surfaces on purpose, and the same flag can mean different things on each, so the
surface is part of the report rather than a detail.

### A bug report that can be acted on

- the exact command, with its flags;
- `ptah version` output;
- the engine and the server version;
- the smallest schema, migration, or config that reproduces it;
- what you expected, and what happened.

### A feature request

Describe the task and the outcome you need rather than the flag you have in
mind. The need often already has a spelling, and where it does not, the shape it
should take depends on which workflow it belongs to.

### Give the issue a label

Issue forms apply a type label for you. When you open an issue another way, add
one type label -- `bug`, `enhancement`, `feature-request`, `documentation`, or
`question` -- and an area label only where it is clear, such as `cli`,
`migration`, `schema-generation`, `postgresql`, `sql`, `security`, or
`constraints`. Engine names have no label of their own; classify by what the
issue is about. Prefer no label to a wrong one.

## Proposing a change

**Open an issue before a change that alters behavior.** Settling the design
before the diff exists is cheaper for everybody, and for anything on the
compatibility surface it is where the question "is this a general capability or
an Atlas adapter?" gets answered.

Work on a branch off `origin/master`, not off a local `master`, which is only as
current as your last pull.

### What a pull request says

Match the repository's existing message style; read `git log` before writing.
The default shape is an imperative subject under about 72 characters, a blank
line, then a body explaining why the change was made. The diff already shows
what changed, so spend the body on the reason, the trade-off, or the risk.

A pull request that adds behavior under the compatibility surface has to say
which it is, `GENERAL CAPABILITY` or `COMPATIBILITY ADAPTER`. AGENTS.md explains
the distinction and what each declaration must name.

### American English, plain international English

Code, comments, documentation, issue and pull request text, and CLI output.
Many readers do not read English as a first language, so a sentence should land
on one pass: short sentences, concrete nouns, active voice.
[docs/STYLE_GUIDE.md](docs/STYLE_GUIDE.md) is the authority and carries the
enumerated rules.

## Building and testing

```bash
make build                       # ptah, ptah-ls, ptah-compat, ptah-integration-test
go test ./... -count=1           # unit contour
make lint                        # golangci-lint, qtlint, nolintguard: three targets, all required
scripts/check-test-style.sh      # declarative-test baseline
make integration-test            # Docker Compose suite; binds fixed host ports
```

`make lint` is not the whole lint job. `.github/workflows/go-lint.yml` runs
policy scripts beside it, and the documentation gates live in
`.github/workflows/docs.yml`. Before you finish, run what the workflows run for
the files you touched; read the job rather than trusting a list in prose.

`make integration-test` and `make docker-clean` act on the whole Docker daemon,
so look at what is already running before you invoke either.

### Tests

Use [quicktest](https://github.com/frankban/quicktest) as `qt`; `depguard`
refuses `testify`. A test function holds no `if`, `switch`, or `goto`; tests are
black-box by default, and split into happy-path and failure-path functions
rather than one table with a `wantErr` branch. AGENTS.md carries the full
standard, including where integration tests live and why a table row holds data
rather than a checker.

### Documentation

A change that affects external behavior updates the documentation in the same
change. That includes CLI names, flags, environment variables, output formats,
exit codes, config keys, generated SQL, public Go APIs, and user-facing errors.
This is a required step, not an opportunistic cleanup.

## Licensing of contributions

Ptah is MIT licensed. By contributing you agree that your contribution is
licensed under the same terms. Do not paste code from another product into this
tree: the implementation is kept clean of third-party product code, which is
what makes the Atlas compatibility work defensible. See
[License boundary](docs/site/src/content/docs/atlas/license-boundary.md).

## Conduct

Participation is covered by the [Code of Conduct](CODE_OF_CONDUCT.md).

## Commercial enquiries

Commercial questions go to `ask@stokaro.com`. Bug reports and feature requests
belong on the issue tracker, where they stay public and get labeled.
