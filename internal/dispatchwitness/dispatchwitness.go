// Package dispatchwitness tells a schema apply run when its first statement
// leaves for the database.
//
// The run's report says that nothing reached the database only while the witness
// has heard nothing, so the signal has to come from where a statement is sent and
// nowhere earlier. Two places send one: the statement loop in
// internal/atlasschema, and a provider session's dispatch in
// internal/providerhost. Both sit behind executor interfaces that have no place
// for a report's concern, so the witness travels on the context, as
// internal/statementtimeout does. A context without one is the common case, a
// rehearsal or a migration, and hears nothing.
package dispatchwitness

import "context"

type key struct{}

// With returns a copy of ctx whose statements call witness as they leave. The
// witness runs on the goroutine that sends, once per send, before the send; a
// run that dies sending has already told it. Nothing takes the signal back: a
// failure after the first statement says nothing about what that statement did.
func With(ctx context.Context, witness func()) context.Context {
	return context.WithValue(ctx, key{}, witness)
}

// Sending tells the witness ctx carries, if any, that a statement is about to
// leave. Call it after the last check that keeps the statement in this process.
func Sending(ctx context.Context) {
	if witness, ok := ctx.Value(key{}).(func()); ok {
		witness()
	}
}
