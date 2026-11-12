package migraterollout

import (
	"fmt"
	"io"
	"strings"

	"ptah.run/internal/migrationrollout"
)

// writePlan prints a plan for a person: each stage in execution order, with its
// settings and its members in start order.
func writePlan(out io.Writer, plan migrationrollout.Plan) {
	fmt.Fprintf(out, "Rollout plan %s\n", plan.Digest)
	for index, stage := range plan.Stages {
		fmt.Fprintf(out, "\nStage %d: %s%s\n", index+1, stage.Name, canaryMark(stage))
		if len(stage.DependsOn) > 0 {
			fmt.Fprintf(out, "  after: %s\n", strings.Join(stage.DependsOn, ", "))
		}
		fmt.Fprintf(out, "  concurrency: %d, on failure: %s, requires: %s (%d of %d)\n",
			stage.Concurrency, stage.OnFailure, stage.Require, stage.Required, len(stage.Targets))
		for _, target := range stage.Targets {
			fmt.Fprintf(out, "  - %s  %s%s\n", target.Name, target.Database, targetExtras(target.TargetRecord))
		}
	}
}

func canaryMark(stage migrationrollout.Stage) string {
	if stage.Canary {
		return " (canary)"
	}
	return ""
}

func targetExtras(target migrationrollout.TargetRecord) string {
	var extras []string
	if target.URLEnv != "" {
		extras = append(extras, "url from $"+target.URLEnv)
	}
	if target.Config != "" {
		extras = append(extras, "config "+target.Config)
	}
	if target.ExpectSequence != "" {
		extras = append(extras, "approved sequence "+target.ExpectSequence)
	}
	if target.AllowDestructive {
		extras = append(extras, "destructive statements allowed")
	}
	if len(extras) == 0 {
		return ""
	}
	return "  (" + strings.Join(extras, "; ") + ")"
}

// writeHeader names what the invocation is bound to before anything runs.
func writeHeader(out io.Writer, state *migrationrollout.State, m mode) {
	fmt.Fprintf(out, "%s rollout %s\n", m.verb(), state.Plan)
	fmt.Fprintf(out, "Release: %s %s (%s)\n", state.Release.Kind, state.Release.Source, state.Release.Digest)
}

// reporter prints a rollout's progress for a person as it happens.
type reporter struct {
	out io.Writer
}

func newReporter(out io.Writer) *reporter { return &reporter{out: out} }

func (r *reporter) observe(event migrationrollout.Event) {
	switch event.Kind {
	case migrationrollout.EventStageStarted:
		fmt.Fprintf(r.out, "Stage %s: running, at most %d at once\n", event.Stage.Name, event.Stage.Concurrency)
	case migrationrollout.EventTargetStarted:
		fmt.Fprintf(r.out, "  %s: running against %s\n", event.Target.Name, event.Target.Database)
	case migrationrollout.EventTargetFinished:
		fmt.Fprintf(r.out, "  %s: %s\n", event.Target.Name, describeTarget(event.Target))
	case migrationrollout.EventTargetReconciled:
		fmt.Fprintf(r.out, "  %s: %s\n", event.Target.Name, describeReconciled(event.Target))
	case migrationrollout.EventStageFinished:
		fmt.Fprintf(r.out, "Stage %s: %s\n", event.Stage.Name, describeStage(event.Stage))
	}
}

// describeTarget is a target's status with the one detail that explains it.
func describeTarget(target migrationrollout.TargetState) string {
	detail := ""
	switch {
	case target.Error != nil:
		detail = firstLine(target.Error.Message)
	case target.Note != "":
		detail = target.Note
	case target.Status == migrationrollout.TargetSucceeded && target.Version != "":
		detail = fmt.Sprintf("applied %d, at version %s", len(target.Applied), target.Version)
	case target.Status == migrationrollout.TargetNoOp && target.Version != "":
		detail = "already at version " + target.Version
	}
	if detail == "" {
		return string(target.Status)
	}
	return string(target.Status) + ": " + detail
}

// describeReconciled is what a resumed run decided about a target it read
// back: the note that says why, or the status it kept.
func describeReconciled(target migrationrollout.TargetState) string {
	if target.Note != "" {
		return target.Note
	}
	if target.Version != "" {
		return fmt.Sprintf("read back as %s, at version %s", target.Status, target.Version)
	}
	return "read back as " + string(target.Status)
}

func describeStage(stage migrationrollout.StageState) string {
	if stage.Status == migrationrollout.StageBlocked {
		return "blocked by " + strings.Join(stage.BlockedBy, ", ")
	}
	return fmt.Sprintf("%s: %d of %d targets succeeded, %d required",
		stage.Status, stage.Succeeded, len(stage.Targets), stage.Required)
}

func firstLine(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	return line
}

// writeSummary closes a run for a person: the run's status, every stage's,
// every target that needs attention, and where the record is.
func writeSummary(out io.Writer, state *migrationrollout.State, path string) {
	fmt.Fprintf(out, "\nRollout %s\n", state.Status)
	for _, stage := range state.Stages {
		fmt.Fprintf(out, "  %s: %s\n", stage.Name, describeStage(stage))
		for _, target := range stage.Targets {
			if target.Status.Failure() || target.Status == migrationrollout.TargetRunning {
				fmt.Fprintf(out, "    %s: %s\n", target.Name, describeTarget(target))
			}
		}
	}
	fmt.Fprintf(out, "State: %s\n", path)
}

// unmetStages names the stages that did not succeed, for the error a failed
// run returns.
func unmetStages(state *migrationrollout.State) string {
	var unmet []string
	for _, stage := range state.Stages {
		if stage.Status != migrationrollout.StageSucceeded {
			unmet = append(unmet, fmt.Sprintf("stage %s %s", stage.Name, stage.Status))
		}
	}
	return strings.Join(unmet, ", ")
}
