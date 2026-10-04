package ydbreplication_test

import (
	"maps"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbreplication"
)

// TestParseReplication_HappyPath reads a replication's connection and
// consistency the way YDB keeps them: the level in lower case, whatever case
// it was written in.
func TestParseReplication_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   ast.AsyncReplicationSpec
	}{
		{
			name:   "a connection alone",
			values: map[string]string{"connection_string": "grpc://primary:2136/?database=/prod"},
			want: ast.AsyncReplicationSpec{Connection: ast.ReplicationConnectionSpec{
				ConnectionString: "grpc://primary:2136/?database=/prod"}},
		},
		{
			name: "a user with a password secret by path, at the global level",
			values: map[string]string{
				"connection_string": "grpcs://primary:2135/?database=/prod", "user": "replicator",
				"password_secret_path": "secrets/replicator", "consistency_level": "GLOBAL", "commit_interval": "PT1.5S",
			},
			want: ast.AsyncReplicationSpec{
				Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpcs://primary:2135/?database=/prod",
					User: "replicator", PasswordSecretPath: "secrets/replicator"},
				ConsistencyLevel: "global", CommitInterval: "PT1.5S",
			},
		},
		{
			name: "a token secret by name",
			values: map[string]string{
				"connection_string": "grpc://primary:2136/?database=/prod", "token_secret_name": "token",
				"consistency_level": "row",
			},
			want: ast.AsyncReplicationSpec{
				Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod",
					TokenSecretName: "token"},
				ConsistencyLevel: "row",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbreplication.ParseReplication(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestParseReplication_FailurePath refuses a replication YDB would refuse, or
// would keep differently from how it was written.
func TestParseReplication_FailurePath(t *testing.T) {
	const conn = "grpc://primary:2136/?database=/prod"
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{name: "no connection", values: make(map[string]string),
			wantErr: `invalid connection_string: a replication reads another database, .*`},
		{name: "a connection string without a scheme", values: map[string]string{"connection_string": "primary:2136/?database=/prod"},
			wantErr: `invalid connection_string "primary:2136/\?database=/prod": takes grpc://.*`},
		{name: "a database in the path", values: map[string]string{"connection_string": "grpc://primary:2136/prod"},
			wantErr: `invalid connection_string "grpc://primary:2136/prod": .*the database goes in the database parameter.*`},
		{name: "no port", values: map[string]string{"connection_string": "grpc://primary/?database=/prod"},
			wantErr: `invalid connection_string .*the address needs a host and a port`},
		{name: "a relative database", values: map[string]string{"connection_string": "grpc://primary:2136/?database=prod"},
			wantErr: `invalid connection_string .*the database is an absolute path such as /local`},
		{name: "another parameter", values: map[string]string{"connection_string": conn + "&token=x"},
			wantErr: `invalid connection_string .*the database parameter is the only one`},
		// #nosec G101 -- the fixture is the credential the parse refuses.
		{name: "credentials in the string", values: map[string]string{"connection_string": "grpc://u:p@primary:2136/?database=/prod"},
			wantErr: `invalid connection_string .*carries credentials.*`},
		{name: "a user without a password secret", values: map[string]string{"connection_string": conn, "user": "u"},
			wantErr: `invalid user "u": a user signs in with a password secret.*`},
		{name: "a password secret without a user", values: map[string]string{"connection_string": conn, "password_secret_name": "p"},
			wantErr: `invalid user: a password secret signs in as a user.*`},
		{name: "two credentials", values: map[string]string{"connection_string": conn, "token_secret_name": "t",
			"user": "u", "password_secret_name": "p"},
			wantErr: `invalid credentials: names more than one secret.*`},
		{name: "a secret name with a leading slash", values: map[string]string{"connection_string": conn,
			"token_secret_name": "/local/t"},
			wantErr: `invalid token_secret_name "/local/t": a secret name holds no leading slash.*token_secret_path`},
		{name: "an absolute secret path", values: map[string]string{"connection_string": conn,
			"token_secret_path": "/local/t"},
			wantErr: `invalid token_secret_path "/local/t": takes a path relative to the database root.*`},
		{name: "an empty credential", values: map[string]string{"connection_string": conn, "user": " "},
			wantErr: `invalid user: names nothing.*`},
		{name: "an unknown level", values: map[string]string{"connection_string": conn, "consistency_level": "strict"},
			wantErr: `invalid consistency_level "strict": takes row or global`},
		{name: "a commit interval at the row level", values: map[string]string{"connection_string": conn,
			"commit_interval": "PT1M"},
			wantErr: `invalid commit_interval "PT1M": a commit interval belongs to the global consistency level.*Ambiguous consistency level.*`},
		{name: "a commit interval finer than a millisecond", values: map[string]string{"connection_string": conn,
			"consistency_level": "global", "commit_interval": "PT0.0015S"},
			wantErr: `invalid commit_interval "PT0.0015S": YDB keeps whole milliseconds.*`},
		{name: "no commit interval at all", values: map[string]string{"connection_string": conn,
			"consistency_level": "global", "commit_interval": "PT0S"},
			wantErr: `invalid commit_interval "PT0S": is no positive time.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbreplication.ParseReplication(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.DeepEquals, ast.AsyncReplicationSpec{})
		})
	}
}

// TestParseItem_HappyPath reads a source of the source database relative or
// absolute, and a target relative to this database's root.
func TestParseItem_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := ydbreplication.ParseItem(map[string]string{"source": " /prod/ledger ", "target": "replica/ledger"})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, ast.AsyncReplicationItem{Source: "/prod/ledger", Target: "replica/ledger"})
}

// TestParseItem_FailurePath refuses an item naming nothing, an absolute target
// and a path YDB would read as another one.
func TestParseItem_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{name: "no source", values: map[string]string{"target": "t"}, wantErr: `invalid source: .*`},
		{name: "no target", values: map[string]string{"source": "s"}, wantErr: `invalid target: .*`},
		{name: "an absolute target", values: map[string]string{"source": "s", "target": "/local/t"},
			wantErr: `invalid target "/local/t": takes a path relative to the database root.*`},
		{name: "a climbing source", values: map[string]string{"source": "../s", "target": "t"},
			wantErr: `invalid source "../s": takes a path of directory and object names.*`},
		{name: "an empty segment", values: map[string]string{"source": "s", "target": "a//t"},
			wantErr: `invalid target "a//t": takes a path of directory and object names.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbreplication.ParseItem(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, ast.AsyncReplicationItem{})
		})
	}
}

// TestParseTransfer_HappyPath reads a transfer of a topic in its own database
// and one of a topic in another, the lambda kept as written.
func TestParseTransfer_HappyPath(t *testing.T) {
	const lambda = "($msg) -> {\n  return [<| id: $msg._offset |>];\n}"
	tests := []struct {
		name   string
		values map[string]string
		want   ast.TransferSpec
	}{
		{
			name:   "a topic of its own database",
			values: map[string]string{"source": "orders/feed", "target": "order_log", "using": lambda},
			want:   ast.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: lambda},
		},
		{
			name: "a topic of another database, every setting named",
			values: map[string]string{
				"source": "/prod/events", "target": "events", "using": lambda, "consumer": "ingest",
				"batch_size_bytes": "1048576", "flush_interval": "PT10S",
				"connection_string": "grpc://primary:2136/?database=/prod", "token_secret_path": "secrets/token",
			},
			want: ast.TransferSpec{
				Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod",
					TokenSecretPath: "secrets/token"},
				Source: "/prod/events", Target: "events", Lambda: lambda, Consumer: "ingest",
				BatchSizeBytes: 1048576, FlushInterval: "PT10S",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbreplication.ParseTransfer(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestParseTransfer_FailurePath refuses a transfer YDB would refuse or keep
// differently, and a lambda Ptah cannot write as the statement's USING.
func TestParseTransfer_FailurePath(t *testing.T) {
	const lambda = "($msg) -> { return []; }"
	base := func(extra map[string]string) map[string]string {
		values := map[string]string{"source": "tp", "target": "t", "using": lambda}
		maps.Copy(values, extra)
		return values
	}
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{name: "no lambda", values: base(map[string]string{"using": ""}),
			wantErr: `invalid using: a transfer turns each message into rows through a lambda.*`},
		{name: "a named lambda", values: base(map[string]string{"using": "$transform"}),
			wantErr: `invalid using "\$transform": takes the lambda written inline.*`},
		{name: "a lambda ending the statement", values: base(map[string]string{"using": lambda + ";"}),
			wantErr: `invalid using .*: ends with a semicolon.*`},
		{name: "no source", values: base(map[string]string{"source": ""}),
			wantErr: `invalid source: a transfer names the topic it reads`},
		{name: "an absolute local topic", values: base(map[string]string{"source": "/local/tp"}),
			wantErr: `invalid source "/local/tp": takes a path relative to the database root.*`},
		{name: "no target", values: base(map[string]string{"target": ""}),
			wantErr: `invalid target: a transfer names the table it writes`},
		{name: "a zero batch", values: base(map[string]string{"batch_size_bytes": "0"}),
			wantErr: `invalid batch_size_bytes "0": takes a whole number of bytes above zero.*`},
		{name: "a fraction of a second", values: base(map[string]string{"flush_interval": "PT1.5S"}),
			wantErr: `invalid flush_interval "PT1.5S": YDB keeps whole seconds.*`},
		{name: "a consumer path", values: base(map[string]string{"consumer": "a/b"}),
			wantErr: `invalid consumer "a/b": takes the name of a consumer of the topic.*`},
		{name: "a credential for a topic of its own database", values: base(map[string]string{"token_secret_name": "t"}),
			wantErr: `invalid connection_string: a credential signs in to another database.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbreplication.ParseTransfer(test.values)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.DeepEquals, ast.TransferSpec{})
		})
	}
}

// TestEndpoint_String writes a connection the way DescribeReplication reads
// it back.
func TestEndpoint_String(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbreplication.Endpoint{Address: "localhost:2136", Database: "/local"}.String(), qt.Equals,
		"grpc://localhost:2136/?database=/local")
	c.Assert(ydbreplication.Endpoint{Secure: true, Address: "h:2135", Database: "/prod"}.String(), qt.Equals,
		"grpcs://h:2135/?database=/prod")
	c.Assert(ydbreplication.CanonicalConnectionString("grpc://localhost:2136?database=/local"), qt.Equals,
		"grpc://localhost:2136/?database=/local")
}

// TestFormatMillis writes an interval as YDB's Interval reads it.
func TestFormatMillis(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbreplication.FormatMillis(60_000), qt.Equals, "PT1M")
	c.Assert(ydbreplication.FormatMillis(1_500), qt.Equals, "PT1.5S")
	c.Assert(ydbreplication.FormatMillis(86_400_000), qt.Equals, "P1D")
}
