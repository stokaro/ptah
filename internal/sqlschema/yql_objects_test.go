package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLViewQuery(t *testing.T) {
	c := qt.New(t)
	body := "SELECT id, ';' AS text FROM `app/items` /* keep the query */ WHERE id > 0"
	database, _, err := sqlschema.Read([]byte("CREATE VIEW `app/summary` WITH (security_invoker = TRUE) AS "+body+"; CREATE TOPIC `app/events`;"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Views, qt.DeepEquals, []schemamodel.View{{Name: "app.summary", Body: body}})
	c.Assert(database.Topics, qt.DeepEquals, []schemamodel.Topic{{Name: "events", Schema: "app"}})
}

func TestReadYQLTopicConsumers(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte("CREATE TOPIC `app/events.v1` (CONSUMER `work\\x65r` WITH (important = TRUE, read_from = Timestamp('2026-01-01T00:00:00Z'), supported_codecs = 'raw,gzip')) WITH (min_active_partitions = 2, retention_period = Interval('P1D'));"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Topics, qt.DeepEquals, []schemamodel.Topic{{Name: "events.v1", Schema: "app", Spec: ast.TopicSpec{
		MinActivePartitions: 2, RetentionPeriod: "P1D", Consumers: []ast.TopicConsumerSpec{{Name: "worker", Important: true, ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}}},
	}}})
}

func TestReadYQLObjectRefusals(t *testing.T) {
	for _, text := range []string{
		"CREATE VIEW v AS SELECT 1;",
		"CREATE VIEW v WITH (SECURITY_INVOKER = TRUE) AS SELECT 1;",
		"CREATE VIEW v WITH (security_invoker = FALSE) AS SELECT 1;",
		"CREATE VIEW v WITH (security_invoker = TRUE, unknown = 1) AS SELECT 1;",
		"CREATE VIEW v WITH (security_invoker = TRUE) AS DELETE FROM t;",
		"CREATE VIEW v WITH (security_invoker = TRUE) AS SELECT (1;",
		"CREATE VIEW v WITH (security_invoker = TRUE) AS DO BEGIN SELECT 1;",
		"CREATE VIEW v WITH (security_invoker = TRUE) AS SELECT 1; DROP TABLE t;",
		"CREATE VIEW `/local/v` WITH (security_invoker = TRUE) AS SELECT 1;",
		"CREATE TOPIC `/local/events`;",
		"CREATE TOPIC events (CONSUMER worker, CONSUMER worker);",
		"CREATE TOPIC events (CONSUMER worker WITH (name = 'other'));",
		"CREATE TOPIC events (CONSUMER worker WITH (important = TRUE, availability_period = Interval('PT1H')));",
		"CREATE TOPIC events WITH (retention_period = 'P1D');",
		"CREATE TOPIC events WITH (retention_period = Interval('P1D') + Interval('P1D'));",
		"CREATE TOPIC events WITH (retention_storage_mb = 100);",
		"CREATE TOPIC events WITH (min_active_partitions = 0);",
		"CREATE TOPIC events WITH (supported_codecs = 'unrecognized');",
		"CREATE TOPIC events WITH (max_active_partitions = 4);",
		"CREATE TOPIC events; UPSERT INTO t (id) VALUES (1);",
	} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(text), "ydb")
			c.Assert(err, qt.ErrorMatches, "YQL schema at position .*")
			c.Assert(statements, qt.IsNil)
			c.Assert(database.Views, qt.HasLen, 0)
			c.Assert(database.Topics, qt.HasLen, 0)
		})
	}
}
