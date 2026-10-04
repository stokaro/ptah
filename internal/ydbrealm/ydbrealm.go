// Package ydbrealm gives a run a YDB dev database of its own: a directory, a
// dev realm, that Ptah creates in the database a URL names and removes when
// the run ends.
//
// SQL cannot create a YDB database, and a cluster's databases are created by
// its administrators, so the dev, shadow and scratch databases the other
// engines get from CREATE DATABASE are directories here. A realm is
// ydburl.RealmDirectory/<name> under the database. A connection opened from
// the URL [Enter] returns sends `PRAGMA TablePathPrefix` with the realm's path
// before every query, and its schema reader and writer read and change only
// what is under the realm, so the run sees an empty database it owns. A
// connection that names no realm leaves the realms' directory out of
// everything it reads, plans and resets, so a realm in the database a run
// targets is never part of the target's schema.
//
// The isolation holds for what Ptah sends and for every statement the dev
// replay guard lets through: the guard refuses a statement that names a path
// outside the realm, climbs out of it, or changes something the whole
// database shares, such as a user or a permission
// (ptah.run/internal/devclean).
package ydbrealm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/ydburl"
)

// nameBytes is the entropy of a realm's name. Two runs that drew the same
// name would share a realm, and a directory that exists is created again
// without an error, so the name carries enough random bytes that a collision
// is not a practical outcome.
const nameBytes = 8

// removeTimeout bounds the removal of a realm. It runs on a context detached
// from the caller's, a canceled one included, so a canceled run still removes
// its realm, and a server that does not answer must not hold the run open.
const removeTimeout = time.Minute

// Applies reports whether rawURL names a YDB database, whose dev databases
// are realms.
func Applies(rawURL string) bool {
	scheme, _, found := strings.Cut(strings.TrimSpace(rawURL), "://")
	return found && platform.NormalizeDialect(scheme) == platform.YDB
}

// Enter creates a realm in the database rawURL names and returns a URL naming
// it, with a release that removes the realm with everything in it. The caller
// must call the release on every path; it is safe to call more than once. A
// URL that names a realm already gets a new one in its place.
//
// The release reports a realm it could not remove in a warning that names the
// directory, which is what an operator removes by hand.
func Enter(ctx context.Context, rawURL string) (string, func(), error) {
	rawURL = strings.TrimSpace(rawURL)
	parsed, err := ydburl.Parse(rawURL)
	if err != nil {
		return "", func() {}, fmt.Errorf("invalid YDB URL: %w", err)
	}
	if parsed.Database == "" {
		return "", func() {}, errors.New("invalid YDB URL: name the database the dev realm is created in")
	}
	databaseURL, err := ydburl.WithoutRealm(rawURL)
	if err != nil {
		return "", func() {}, err
	}
	name, err := realmName()
	if err != nil {
		return "", func() {}, err
	}
	realmURL, err := ydburl.WithRealm(rawURL, name)
	if err != nil {
		return "", func() {}, err
	}
	if err := create(ctx, databaseURL, name); err != nil {
		return "", func() {}, fmt.Errorf("create the dev realm %s in %s: %w",
			name, parsed.Database, err)
	}
	directory := path.Join(parsed.Database, ydburl.RealmDirectory, name)
	release := sync.OnceFunc(func() {
		removeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), removeTimeout)
		defer cancel()
		if err := Remove(removeCtx, databaseURL, name); err != nil {
			slog.Warn("failed to remove the YDB dev realm; remove the directory by hand",
				"directory", directory, "error", err)
		}
	})
	return realmURL, release, nil
}

// realmName draws the name of a new realm.
func realmName() (string, error) {
	raw := make([]byte, nameBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate a dev realm name: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

// directoryMaker is what creating a realm needs of a YDB connection's writer.
type directoryMaker interface {
	MakeDirectory(ctx context.Context, dir string) error
}

// realmRemover is what removing a realm needs of a YDB connection's writer.
type realmRemover interface {
	RemoveRealm(ctx context.Context, realm string) error
}

// create makes the realm's directory through a connection to the database.
func create(ctx context.Context, databaseURL, name string) (err error) {
	conn, err := dbschema.ConnectToServer(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	maker, ok := conn.SchemaWriter().(directoryMaker)
	if !ok {
		return fmt.Errorf("the connection's schema writer %T cannot create a directory", conn.SchemaWriter())
	}
	return maker.MakeDirectory(ctx, path.Join(ydburl.RealmDirectory, name))
}

// Remove removes the realm name, with everything in it, from the database
// databaseURL names, and the realms' directory when no realm is left in it. A
// realm that does not exist is removed already.
func Remove(ctx context.Context, databaseURL, name string) (err error) {
	conn, err := dbschema.ConnectToServer(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	remover, ok := conn.SchemaWriter().(realmRemover)
	if !ok {
		return fmt.Errorf("the connection's schema writer %T cannot remove a dev realm", conn.SchemaWriter())
	}
	return remover.RemoveRealm(ctx, name)
}
