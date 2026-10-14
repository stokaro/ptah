package embedbackend_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedbackend"
	"ptah.run/internal/embedcatchup"
	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// prepareSpec is a specification a prepare is asked for.
func prepareSpec() embedgen.Spec {
	return embedgen.Spec{
		Source: embedgen.Source{Table: "articles", KeyFields: []string{"id"}, InputFields: []string{"title"}},
		Model:  embedgen.Model{Identifier: "m", Revision: "1", ReportedDimension: 4},
		Target: embedgen.Target{Table: "articles", Column: "embedding", Representation: "float"},
	}
}

// preparedGeneration is the registry row prepare writes for it.
func preparedGeneration() embedstore.Generation {
	return embedstore.Generation{
		Identity: prepareSpec().Identity().Digest, Dimension: 4,
		TargetTable: "articles", TargetColumn: "embedding", SourceTable: "articles",
		ConsistencyMode: string(embedcatchup.ModeImmutable),
	}
}

// preparedRun is the run prepare creates for it.
func preparedRun() embedrun.Run {
	return embedrun.Run{
		ID: "run-1", SpecDigest: prepareSpec().Identity().Digest, GenerationIdentity: prepareSpec().Identity().Digest,
		Source: embedstore.SourceIdentity("", "articles"), Target: "articles.embedding",
		Phase: embedrun.PhasePrepared, Status: embedrun.StatusRunning,
	}
}

func TestValidatePrepare_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(embedbackend.ValidatePrepare(
		prepareSpec(), preparedGeneration(), preparedRun(), embedcatchup.ModeImmutable), qt.IsNil)
}

// Every argument that disagrees with another is refused before an engine is
// asked anything.
func TestValidatePrepare_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*embedstore.Generation, *embedrun.Run)
		mode    embedcatchup.Mode
		wantErr error
	}{
		{name: "another generation", mutate: func(g *embedstore.Generation, _ *embedrun.Run) { g.Identity = "other" },
			mode: embedcatchup.ModeImmutable, wantErr: embedrun.ErrGeneration},
		{name: "a run for another generation", mutate: func(_ *embedstore.Generation, r *embedrun.Run) {
			r.GenerationIdentity = "other"
		}, mode: embedcatchup.ModeImmutable, wantErr: embedrun.ErrGeneration},
		{name: "a run past prepare", mutate: func(_ *embedstore.Generation, r *embedrun.Run) {
			r.Phase = embedrun.PhaseBackfilling
		}, mode: embedcatchup.ModeImmutable, wantErr: embedrun.ErrPhase},
		{name: "an abandoned run", mutate: func(_ *embedstore.Generation, r *embedrun.Run) {
			r.Status = embedrun.StatusAbandoned
		}, mode: embedcatchup.ModeImmutable, wantErr: embedrun.ErrTerminal},
		{name: "another mode", mutate: func(*embedstore.Generation, *embedrun.Run) {},
			mode: embedcatchup.ModeOutbox, wantErr: embedstore.ErrConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generation, run := preparedGeneration(), preparedRun()
			test.mutate(&generation, &run)
			c.Assert(embedbackend.ValidatePrepare(prepareSpec(), generation, run, test.mode), qt.ErrorIs, test.wantErr)
		})
	}
}

// A run that already exists is a finished prepare only when it reached its
// boundary and its registry row records what is prepared now.
func TestValidatePreparedRun_HappyPath(t *testing.T) {
	c := qt.New(t)
	existing := preparedRun()
	existing.Phase = embedrun.PhaseBoundaryCaptured

	c.Assert(embedbackend.ValidatePreparedRun(preparedGeneration(), existing), qt.IsNil)
	c.Assert(embedbackend.ValidatePreparedRegistration(
		preparedGeneration(), preparedGeneration(), preparedRun(), existing), qt.IsNil)
}

func TestValidatePreparedRun_FailurePath(t *testing.T) {
	c := qt.New(t)
	unprepared := preparedRun()
	c.Assert(embedbackend.ValidatePreparedRun(preparedGeneration(), unprepared), qt.ErrorMatches,
		`prepare run run-1: phase transition is not on the lifecycle path: existing run is at prepared and has no `+
			`committed prepare boundary`)

	existing := preparedRun()
	existing.Phase = embedrun.PhaseBoundaryCaptured
	moved := preparedGeneration()
	moved.TargetColumn = "embedding_v2"
	c.Assert(embedbackend.ValidatePreparedRegistration(preparedGeneration(), moved, preparedRun(), existing),
		qt.ErrorMatches, `the state changed underneath this write: generation .* records target column `+
			`"embedding_v2", prepare requested "embedding"`)
	retired := preparedGeneration()
	retired.RetiredAt = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	c.Assert(embedbackend.ValidatePreparedRegistration(preparedGeneration(), retired, preparedRun(), existing),
		qt.ErrorIs, embedstore.ErrRetired)
	otherSpec := preparedRun()
	otherSpec.SpecDigest = "another"
	c.Assert(embedbackend.ValidatePreparedRegistration(preparedGeneration(), preparedGeneration(), otherSpec, existing),
		qt.ErrorMatches, `.*run run-1 records specification .*, prepare requested another`)
}

// A destruction drops a generation's columns or its table, never both.
func TestRetirementDestruction_Validate(t *testing.T) {
	c := qt.New(t)
	c.Assert(embedbackend.RetirementDestruction{DropColumns: true}.Validate(), qt.IsNil)
	c.Assert(embedbackend.RetirementDestruction{DropTable: true}.Validate(), qt.IsNil)
	c.Assert(embedbackend.RetirementDestruction{DropColumns: true, DropTable: true}.Validate(),
		qt.ErrorIs, embedstore.ErrConflict)
}

// A generation that recorded no specification cannot be measured, and the
// refusal names the question its caller was answering.
func TestRecordedSpec_FailurePath(t *testing.T) {
	c := qt.New(t)
	loaded, err := embedbackend.RecordedSpec(embedstore.Generation{Identity: "gen-1"}, "still fresh")
	c.Assert(err, qt.ErrorMatches,
		`generation gen-1 records no specification, so nothing can measure whether it is still fresh`)
	c.Assert(loaded.Document, qt.IsNil)
}

func TestHasIndex(t *testing.T) {
	c := qt.New(t)
	spec := prepareSpec()
	c.Assert(embedbackend.HasIndex(spec), qt.IsFalse)
	spec.Target.IndexMethod = " vector_kmeans_tree "
	c.Assert(embedbackend.HasIndex(spec), qt.IsTrue)
}

func TestRelation_String(t *testing.T) {
	c := qt.New(t)
	c.Assert(embedbackend.Relation{Table: "articles"}.String(), qt.Equals, "articles")
	c.Assert(embedbackend.Relation{Schema: "app", Table: "articles"}.String(), qt.Equals, "app.articles")
}
