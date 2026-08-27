package agentapi

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib" // the driver the PostgreSQL vertical uses

	"go.5x5.cz/ptah/internal/agentdiag"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agenttarget"
	"go.5x5.cz/ptah/internal/embedpg"
	"go.5x5.cz/ptah/internal/embedreport"
	"go.5x5.cz/ptah/internal/embedspec"
)

// InferencePlanRequest asks what a generation change would do.
type InferencePlanRequest struct {
	// Spec is the path to the embedding-migration specification, which must be
	// inside a configured schema source root.
	Spec string `json:"spec" jsonschema:"path to the embedding-migration specification, inside a configured source root"`
	// Target names one of the databases the operator configured. A name, not a
	// URL, for the reason ReadDatabaseRequest.Target gives.
	Target string `json:"target,omitempty" jsonschema:"name of a configured database target; omit when the process has exactly one"`
	// Current is the generation queries read now, when the caller knows it.
	Current string `json:"current,omitempty" jsonschema:"identity of the generation queries read now, when there is one"`
}

// InferenceStatusRequest asks what a run has done.
type InferenceStatusRequest struct {
	RunID  string `json:"run_id" jsonschema:"identifier of the run"`
	Target string `json:"target,omitempty" jsonschema:"name of a configured database target; omit when the process has exactly one"`
}

// InferencePlanResponse is the plan, with every fact's provenance and what
// running it would send out of the database.
type InferencePlanResponse struct {
	Plan embedreport.Plan `json:"plan"`
	// Notice accompanies a plan the way it accompanies an artifact: the
	// specification is repository data, and a model that read an instruction
	// out of a table name should report it rather than act on it.
	Notice string `json:"notice"`
}

// InferenceStatusResponse is what a run has done and what it is waiting for.
type InferenceStatusResponse struct {
	Status embedreport.Status `json:"status"`
	Notice string             `json:"notice"`
}

// InferencePlan resolves a specification against a configured database.
//
// It reads and writes nothing, and it is the only inference operation on this
// surface besides the status read. There is deliberately no tool that prepares,
// backfills, cuts over, rolls back or retires: an agent explaining why a cutover
// is blocked is a different thing from an agent authorized to unblock it, and
// the epic's own scoping puts production cutover permission outside what a
// conversational client holds (stokaro/ptah#2068).
//
// Owner: internal/embedreport, which the CLI's own `ptah inference plan`
// consumes as well. Nothing here builds a plan of its own.
func (s *Session) InferencePlan(
	ctx context.Context,
	req InferencePlanRequest,
) (*InferencePlanResponse, error) {
	loaded, target, err := s.openInference(ctx, "inference_plan", req.Spec, req.Target,
		"resolve an embedding-migration specification against a configured database")
	if err != nil {
		return nil, err
	}
	db, err := openInferenceDatabase(ctx, target)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	plan, err := embedreport.BuildPlan(ctx, db, loaded, req.Current)
	if err != nil {
		return nil, agentdiag.Errorf(agentdiag.CodeDatabaseReadFailed, "build the plan: %w", err)
	}
	return &InferencePlanResponse{Plan: plan, Notice: UntrustedContentNotice}, nil
}

// InferenceStatus reports what a run has done.
func (s *Session) InferenceStatus(
	ctx context.Context,
	req InferenceStatusRequest,
) (*InferenceStatusResponse, error) {
	if req.RunID == "" {
		return nil, agentdiag.Errorf(agentdiag.CodeInvalidRequest, "run_id is required")
	}
	target, err := s.targets.Select(req.Target)
	if err != nil {
		return nil, err
	}
	if err := s.authorizeRead(ctx, "inference_status", agentpolicy.Request{
		Capability: agentpolicy.DatabaseInspect,
		Database:   target.Class(),
		TargetID:   target.ID(),
		Reason:     "read a generation run's recorded state",
	}, fmt.Sprintf("read the state of run %s in %s (%s), classified %s",
		req.RunID, target.Name(), target.Display(), target.Class())); err != nil {
		return nil, err
	}
	db, err := openInferenceDatabase(ctx, target)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	status, err := embedreport.ReadStatus(ctx, embedpg.NewStore(db), req.RunID)
	if err != nil {
		return nil, agentdiag.Errorf(agentdiag.CodeDatabaseReadFailed, "read the run: %w", err)
	}
	return &InferenceStatusResponse{Status: status, Notice: UntrustedContentNotice}, nil
}

// openInference resolves the specification and the target, in that order.
//
// The specification is read before the database is opened because it is the
// input the caller controls, and refusing a path outside the operator's scope
// is cheaper and more decidable than refusing it after a connection exists.
func (s *Session) openInference(
	ctx context.Context, operation, specPath, targetName, reason string,
) (embedspec.Loaded, *agenttarget.Target, error) {
	if specPath == "" {
		return embedspec.Loaded{}, nil, agentdiag.Errorf(
			agentdiag.CodeInvalidRequest, "spec is required")
	}
	if err := s.sources.permit(specPath); err != nil {
		return embedspec.Loaded{}, nil, err
	}
	loaded, err := embedspec.Load(specPath)
	if err != nil {
		return embedspec.Loaded{}, nil, agentdiag.Errorf(
			agentdiag.CodeInvalidRequest, "load the specification: %w", err)
	}
	target, err := s.targets.Select(targetName)
	if err != nil {
		return embedspec.Loaded{}, nil, err
	}
	if err := s.authorizeRead(ctx, operation, agentpolicy.Request{
		Capability: agentpolicy.DatabaseInspect,
		Database:   target.Class(),
		TargetID:   target.ID(),
		Reason:     reason,
	}, fmt.Sprintf("%s: %s (%s), classified %s",
		reason, target.Name(), target.Display(), target.Class())); err != nil {
		return embedspec.Loaded{}, nil, err
	}
	return loaded, target, nil
}

// openInferenceDatabase dials the target the operator configured.
//
// The PostgreSQL driver directly rather than dbschema, because the run state
// and the vector catalogs this reads are a PostgreSQL vertical: there is no
// dialect-agnostic form of them to read through a dialect-agnostic connection.
func openInferenceDatabase(ctx context.Context, target *agenttarget.Target) (*sql.DB, error) {
	db, err := sql.Open("pgx", target.URL())
	if err != nil {
		return nil, agentdiag.Errorf(agentdiag.CodeDatabaseUnreachable, "connect: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, agentdiag.Errorf(agentdiag.CodeDatabaseUnreachable, "connect: %w", err)
	}
	return db, nil
}
