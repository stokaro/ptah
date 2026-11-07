//go:build integration

package partitionof_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// partitionedEvents is a range-partitioned table with a primary key, an index and
// two partitions, declared with PARTITION OF.
const partitionedEvents = `
CREATE TABLE events (id bigint NOT NULL, at date NOT NULL, note text DEFAULT 'x', PRIMARY KEY (id, at)) PARTITION BY RANGE (at);
CREATE TABLE events_2026 PARTITION OF events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
CREATE TABLE events_rest PARTITION OF events DEFAULT;
CREATE INDEX events_note ON events (note);
`

// postgresDumpOfEvents is what pg_dump 18 writes for partitionedEvents.
const postgresDumpOfEvents = `\restrict bOMhEXnexOW3tvDg5NTTwP1FGM9GPjnB7sgGYWvMBgICQ0H5YUab8jmZmEQvvvb

CREATE TABLE public.events (
    id bigint NOT NULL,
    at date NOT NULL,
    note text DEFAULT 'x'::text
)
PARTITION BY RANGE (at);
CREATE TABLE public.events_2026 (
    id bigint CONSTRAINT events_id_not_null NOT NULL,
    at date CONSTRAINT events_at_not_null NOT NULL,
    note text DEFAULT 'x'::text
);
CREATE TABLE public.events_rest (
    id bigint CONSTRAINT events_id_not_null NOT NULL,
    at date CONSTRAINT events_at_not_null NOT NULL,
    note text DEFAULT 'x'::text
);
ALTER TABLE ONLY public.events ATTACH PARTITION public.events_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
ALTER TABLE ONLY public.events ATTACH PARTITION public.events_rest DEFAULT;
ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (id, at);
ALTER TABLE ONLY public.events_2026
    ADD CONSTRAINT events_2026_pkey PRIMARY KEY (id, at);
ALTER TABLE ONLY public.events_rest
    ADD CONSTRAINT events_rest_pkey PRIMARY KEY (id, at);
CREATE INDEX events_note ON ONLY public.events USING btree (note);
CREATE INDEX events_2026_note_idx ON public.events_2026 USING btree (note);
CREATE INDEX events_rest_note_idx ON public.events_rest USING btree (note);
ALTER INDEX public.events_note ATTACH PARTITION public.events_2026_note_idx;
ALTER INDEX public.events_pkey ATTACH PARTITION public.events_2026_pkey;
ALTER INDEX public.events_note ATTACH PARTITION public.events_rest_note_idx;
ALTER INDEX public.events_pkey ATTACH PARTITION public.events_rest_pkey;

\unrestrict bOMhEXnexOW3tvDg5NTTwP1FGM9GPjnB7sgGYWvMBgICQ0H5YUab8jmZmEQvvvb
`

// partitionedRegions is a list-partitioned table with two partitions and no
// index, declared with PARTITION OF.
const partitionedRegions = `
CREATE TABLE regions (code text NOT NULL, name text) PARTITION BY LIST (code);
CREATE TABLE regions_eu PARTITION OF regions FOR VALUES IN ('de', 'fr');
CREATE TABLE regions_rest PARTITION OF regions DEFAULT;
`

// yugabyteDumpOfRegions is what a PostgreSQL 15 dump writes for
// partitionedRegions: no restrict lines, and no NOT NULL names, which that
// release does not keep. YugabyteDB's grammar reads no ALTER INDEX
// (stokaro/ptah#5716), so the table has no index for a dump to attach.
const yugabyteDumpOfRegions = `
CREATE TABLE public.regions (
    code text NOT NULL,
    name text
)
PARTITION BY LIST (code);
CREATE TABLE public.regions_eu (
    code text NOT NULL,
    name text
);
CREATE TABLE public.regions_rest (
    code text NOT NULL,
    name text
);
ALTER TABLE ONLY public.regions ATTACH PARTITION public.regions_eu FOR VALUES IN ('de', 'fr');
ALTER TABLE ONLY public.regions ATTACH PARTITION public.regions_rest DEFAULT;
`

// TestADumpOfPartitionsReadsSyncedE2E reads a dump of a partitioned table against
// the database it describes: plan, compare and drift all find the two the same.
func TestADumpOfPartitionsReadsSyncedE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
		built  string
		dump   string
	}{
		{name: "postgres", engine: dbtarget.PostgreSQL, built: partitionedEvents, dump: postgresDumpOfEvents},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, built: partitionedRegions, dump: yugabyteDumpOfRegions},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dev := newScratch(c, test.engine)
			dev.exec(c, test.built)

			dev.assertSynced(c, test.dump)
		})
	}
}

// TestADumpOfPartitionsAppliesE2E builds a database from the dump and reads the
// catalog back: each partition is attached to its parent with its bound, a row
// lands in the partition its bound selects, and the next plan is synced.
func TestADumpOfPartitionsAppliesE2E(t *testing.T) {
	c := qt.New(t)
	dev := newScratch(c, dbtarget.PostgreSQL)

	dev.apply(c, postgresDumpOfEvents)

	c.Assert(dev.partitions(c), qt.DeepEquals, map[string]string{
		"public.events_2026": "public.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
		"public.events_rest": "public.events DEFAULT",
	})
	dev.exec(c, "INSERT INTO events (id, at) VALUES (1, '2026-06-01'), (2, '2030-06-01')")
	c.Assert(dev.rowHome(c, 1), qt.Equals, "events_2026")
	c.Assert(dev.rowHome(c, 2), qt.Equals, "events_rest")
	c.Assert(dev.count(c, "SELECT count(*) FROM pg_indexes WHERE tablename = 'events_2026'"), qt.Equals, 2)
	dev.assertSynced(c, postgresDumpOfEvents)
}
