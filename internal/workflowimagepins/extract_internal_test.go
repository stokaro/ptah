package workflowimagepins

// White-box testing required: these tests exercise the unexported YAML-node
// and docker-operand seams that together define the exported File inventory.
// Black-box tests could only observe the final list and would not discriminate
// which parser boundary rejected malformed workflow structure.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestFileInventoriesOnlyExecutableContainerContexts(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    env:
      image: ignored/env:1
    container: direct/container:1
    services:
      quoted:
        "image": quoted/service:2
      flow: {image: flow/service:3}
    steps:
      - run: docker run inline/run:4
      - run: >-
          docker run --detach
          folded/run:5
      - run: |
          docker run --name multiline \
            multiline/run:6 \
            serve
      - run: docker    run spaced/run:7
      - run: echo argument/run:7
`)

	images, err := File(path)

	c.Assert(err, qt.IsNil)
	c.Check(images, qt.DeepEquals, []string{
		"direct/container:1",
		"quoted/service:2",
		"flow/service:3",
		"inline/run:4",
		"folded/run:5",
		"multiline/run:6",
		"spaced/run:7",
	})
}

func TestFileRejectsUnknownAttachedDockerOption(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: docker run --definitely-invalid=value example/database:1.2.3 || true
`)

	_, err := File(path)

	c.Check(err, qt.ErrorMatches, `.*:5: unsupported docker run option "--definitely-invalid=value"`)
}

func TestFileIgnoresStaticallyDisabledSteps(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - if: ${{ false }}
        run: docker run disabled/expression:1
      - if: false
        run: docker run disabled/boolean:2
      - if: ${{ matrix.enabled }}
        run: docker run dynamic/enabled:3
`)

	images, err := File(path)

	c.Assert(err, qt.IsNil)
	c.Check(images, qt.DeepEquals, []string{"dynamic/enabled:3"})
}

func TestFileIgnoresStaticallyDisabledJobs(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  disabled-expression:
    if: ${{ false }}
    services:
      database:
        image: disabled/service:1
    steps:
      - run: docker run disabled/run:2
  disabled-boolean:
    if: false
    container: disabled/container:3
  dynamic:
    if: ${{ github.event_name == 'push' }}
    container: dynamic/container:4
  enabled:
    steps:
      - run: docker run enabled/run:5
`)

	images, err := File(path)

	c.Assert(err, qt.IsNil)
	c.Check(images, qt.DeepEquals, []string{
		"dynamic/container:4",
		"enabled/run:5",
	})
}

func TestFileRejectsNonScalarJobCondition(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    if:
      - false
    container: example/database:1.2.3
`)

	_, err := File(path)

	c.Check(err, qt.ErrorMatches, `.*:5: job test if must be a scalar`)
}

func TestFileAcceptsCombinedBooleanDockerOptions(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: docker run -it combined/interactive:1
      - run: docker run -dit combined/detached:2
`)

	images, err := File(path)

	c.Assert(err, qt.IsNil)
	c.Check(images, qt.DeepEquals, []string{
		"combined/interactive:1",
		"combined/detached:2",
	})
}

func TestFileIgnoresHeredocPayloads(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: |
          cat >start.sh <<'EOF'
          docker run heredoc/first:1
          docker run heredoc/second:2
          EOF
          docker run after/heredoc:3
      - run: |
          docker run --rm opener/image:4 sh <<-SCRIPT
          docker run heredoc/dashed:5
          SCRIPT
      - run: |
          tee first.txt second.txt <<A <<B
          docker run heredoc/a:6
          A
          docker run heredoc/b:7
          B
          docker run after/both:8
`)

	images, err := File(path)

	c.Assert(err, qt.IsNil)
	c.Check(images, qt.DeepEquals, []string{
		"after/heredoc:3",
		"opener/image:4",
		"after/both:8",
	})
}

func TestFileIgnoresHeredocPayloadOfContinuedCommand(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: |
          cat <<'EOF' \
            >start.sh
          docker run heredoc/continued:1
          EOF
          docker run after/continued:2
`)

	images, err := File(path)

	c.Assert(err, qt.IsNil)
	c.Check(images, qt.DeepEquals, []string{"after/continued:2"})
}

func TestFileRejectsUnterminatedHeredoc(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: |
          cat >start.sh <<'EOF'
          docker run never/terminated:1
`)

	_, err := File(path)

	c.Check(err, qt.ErrorMatches, `.*:5: heredoc delimiter "EOF" is never terminated`)
}

func TestFileInventoriesCommandsAfterHerestring(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: |
          grep -q ready <<<"$status"
          docker run after/herestring:1
`)

	images, err := File(path)

	c.Assert(err, qt.IsNil)
	c.Check(images, qt.DeepEquals, []string{"after/herestring:1"})
}

func TestFileInventoriesCommandsAfterQuotedRedirectionText(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: |
          echo "writes a << b line"
          docker run after/quoted:1
`)

	images, err := File(path)

	c.Assert(err, qt.IsNil)
	c.Check(images, qt.DeepEquals, []string{"after/quoted:1"})
}

func TestFileRejectsDuplicateRunKey(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: echo ignored
        "run": docker run example/database:1.2.3
`)

	_, err := File(path)

	c.Check(err, qt.ErrorMatches, `.*:6: duplicate run key`)
}

func TestFileRejectsMergeKeys(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
defaults: &defaults
  image: example/database:1.2.3
jobs:
  test:
    services:
      database:
        <<: *defaults
`)

	_, err := File(path)

	c.Check(err, qt.ErrorMatches, `.*:8: YAML merge keys are not supported`)
}

func TestFileRejectsServiceWithoutImageMapping(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    services:
      database: example/database:1.2.3
`)

	_, err := File(path)

	c.Check(err, qt.ErrorMatches, `.*:5: service database must be a mapping`)
}

func TestFileRejectsDuplicateJobKey(t *testing.T) {
	c := qt.New(t)
	path := writeWorkflow(c, `
jobs:
  test:
    steps:
      - run: true
  test:
    steps:
      - run: docker run example/database:1.2.3
`)

	_, err := File(path)

	c.Check(err, qt.ErrorMatches, `.*:6: duplicate test key`)
}

func writeWorkflow(c *qt.C, contents string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "test.yml")
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
	return path
}
