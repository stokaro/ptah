package capabilityprobe

import (
	"context"
	"fmt"
	"time"

	"go.5x5.cz/ptah/dbschema"
)

// waitInterval is how long WaitForServer sleeps between attempts.
const waitInterval = 2 * time.Second

// WaitForServer blocks until a server answers, or the timeout expires.
//
// It exists so the pipeline does not need a readiness recipe per engine. The
// integration workflow carries one apiece — pg_isready, mysqladmin ping, an
// HTTP /ping, two docker exec invocations — and each is a second way of
// spelling the question the probe is about to ask anyway. Retrying the real
// connection asks it once: a server that can be connected to and can report
// its version banner is a server the probe can measure.
//
// A failure names the last error rather than only the timeout. "Timed out
// after 5m" sends a reader to the wrong place when the truth is that the
// password was wrong on every one of the 150 attempts.
func WaitForServer(ctx context.Context, dbURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	attempts := 0
	for {
		attempts++
		conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
		if err == nil {
			dbschema.CloseAndWarn(conn)
			return nil
		}
		if remaining := time.Until(deadline); remaining <= 0 {
			return fmt.Errorf("%s did not answer within %s (%d attempts); the last error was: %w",
				dbschema.FormatDatabaseURL(dbURL), timeout, attempts, err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s: %w (the last connection error was: %w)",
				dbschema.FormatDatabaseURL(dbURL), ctx.Err(), err)
		case <-time.After(waitInterval):
		}
	}
}
