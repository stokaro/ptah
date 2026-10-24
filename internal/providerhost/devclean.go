package providerhost

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	providerruntime "github.com/stokaro/ptah/provider-sdk/runtime"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/devclean/replay"
)

// A dev database a provider serves is cleaned by the provider: the host
// decides that the database is a dev database, the provider lists and removes
// what it holds. The clean check and the reset read one set, so a database
// judged clean is one the reset leaves as it is.

// offers reports whether the provider and the target's engine both declare a
// feature.
func (t *target) offers(feature string) bool {
	return slices.Contains(t.host.description.GetFeatures(), feature) && slices.Contains(t.engine.GetFeatures(), feature)
}

// ResetObjects lists every object a cleanup of the schemas the scope names,
// or of the whole realm when it names none, would remove or stop at.
func (s *session) ResetObjects(ctx context.Context, scope dbreset.Scope) ([]dbreset.Object, error) {
	if err := s.requireCleanup(); err != nil {
		return nil, err
	}
	listed, err := s.target.host.client.ListDevObjects(ctx, &protocol.ListDevObjectsRequest{SessionId: s.remote.GetId(), Schemas: slices.Clone(scope.Schemas)})
	if err != nil {
		return nil, err
	}
	objects := make([]dbreset.Object, 0, len(listed.GetObjects()))
	for _, object := range listed.GetObjects() {
		ref := object.GetObject()
		objects = append(objects, dbreset.Object{Kind: ref.GetKind(), Schema: ref.GetSchema(), Name: ref.GetName()})
	}
	return objects, nil
}

// DropAllTables empties the session's schema through the provider.
func (s *session) DropAllTables(ctx context.Context) error {
	if s.IsDryRun() {
		return nil
	}
	var schemas []string
	if schema := strings.TrimSpace(s.info.Schema); schema != "" {
		schemas = []string{schema}
	}
	return s.cleanDev(ctx, schemas)
}

// DropDatabaseRealm empties the whole realm the session treats as its
// database through the provider.
func (s *session) DropDatabaseRealm(ctx context.Context) error {
	if s.IsDryRun() {
		return nil
	}
	return s.cleanDev(ctx, nil)
}

func (s *session) requireCleanup() error {
	if s.closed.Load() {
		return fmt.Errorf("provider session is closed")
	}
	if !s.target.offers(provider.DevCleanupFeature) {
		return unsupported("database cleanup")
	}
	return nil
}

func (s *session) cleanDev(ctx context.Context, schemas []string) error {
	if err := s.requireCleanup(); err != nil {
		return err
	}
	result, err := s.target.host.client.CleanDevDatabase(ctx, &protocol.CleanDevRequest{SessionId: s.remote.GetId(), RequestId: rand.Text(), Schemas: schemas})
	if err == nil {
		return nil
	}
	if refused, confirmed := errors.AsType[*provider.DevCleanupError](err); confirmed && result != nil && refused.Outcome == protocol.DevCleanup_REFUSED {
		blocking := make([]string, 0, len(result.GetBlocking()))
		for _, object := range result.GetBlocking() {
			ref := object.GetObject()
			blocking = append(blocking, fmt.Sprintf("%s %q: %s", ref.GetKind(), ref.GetName(), object.GetRefusal()))
		}
		return fmt.Errorf("the provider removed nothing from the dev database, because it cannot remove %s", strings.Join(blocking, "; "))
	}
	return fmt.Errorf("the provider did not confirm the dev database cleanup, so the database is not clean: %w", err)
}

// ReplayGuard returns the provider's replay guard for the session's engine,
// or nil when the engine declares none, which leaves the replay refused.
func (s *session) ReplayGuard() replay.Guard {
	if !s.target.offers(provider.ReplayGuardFeature) {
		return nil
	}
	return replayGuard{target: s.target}
}

type replayGuard struct{ target *target }

// Validate asks the provider, offline, whether the statement's effects stay
// inside scope. A provider that cannot answer refuses the statement.
func (g replayGuard) Validate(stmt string, scope replay.Scope) error {
	ctx, cancel := context.WithTimeout(context.Background(), providerruntime.CallTimeout)
	defer cancel()
	engine := g.target.engine.GetId()
	guarded, err := g.target.host.client.GuardReplay(ctx, &protocol.GuardReplayRequest{Engine: engine, Statements: []string{stmt},
		Scope: &protocol.ReplayScope{Database: scope.Database, Server: scope.Server, Provisioned: scope.Provisioned, Databases: scope.Databases, Session: scope.Session}})
	if err != nil {
		return replay.Refuse(engine, "a statement the provider could not judge")
	}
	if verdict := guarded.GetVerdicts()[0]; verdict.GetRefused() {
		return replay.Refuse(engine, verdict.GetOperation())
	}
	return nil
}

// RealmIdentity names the dev database the session selected, as the provider
// reports it, so two sessions are one realm when their provider names one
// database for both. The provider owns its addresses and the host reads none
// of them; a database of one name on two servers therefore reads as one
// realm, which refuses a dev database rather than reset the target.
func (s *session) RealmIdentity(context.Context) (string, error) {
	database := s.remote.GetDatabase()
	if database == "" {
		return "", fmt.Errorf("the provider names no database for the session")
	}
	return s.target.host.description.GetProviderId() + "|" + s.target.engine.GetId() + "|" + database, nil
}
