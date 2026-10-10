package ydbreplication_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbreplication"
)

// replicationOf is a replication of /prod with the items given.
func replicationOf(items ...ydbreplication.Item) ydbreplication.ReplicationSpec {
	return ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      items,
	}
}

// TestCompareReplication_Equal holds two descriptions of one replication equal
// where YDB keeps them alike: a level and a commit interval named at their
// defaults, a connection string spelled without the slash before the query,
// a source named by its absolute path, items in another order, and a
// directory item read back as one item per table.
func TestCompareReplication_Equal(t *testing.T) {
	tests := []struct {
		name    string
		desired ydbreplication.ReplicationSpec
		current ydbreplication.ReplicationSpec
	}{
		{
			name: "defaults named and left out",
			desired: func() ydbreplication.ReplicationSpec {
				spec := replicationOf(ydbreplication.Item{Source: "a", Target: "ra"})
				spec.ConsistencyLevel = "row"
				return spec
			}(),
			current: replicationOf(ydbreplication.Item{Source: "a", Target: "ra"}),
		},
		{
			name: "a global level's default commit interval",
			desired: func() ydbreplication.ReplicationSpec {
				spec := replicationOf(ydbreplication.Item{Source: "a", Target: "ra"})
				spec.ConsistencyLevel, spec.CommitInterval = "global", "PT10S"
				return spec
			}(),
			current: func() ydbreplication.ReplicationSpec {
				spec := replicationOf(ydbreplication.Item{Source: "a", Target: "ra"})
				spec.ConsistencyLevel = "global"
				return spec
			}(),
		},
		{
			name: "a connection string without the slash and an absolute source",
			desired: ydbreplication.ReplicationSpec{
				Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136?database=/prod"},
				Items:      []ydbreplication.Item{{Source: "/prod/a", Target: "ra"}},
			},
			current: replicationOf(ydbreplication.Item{Source: "a", Target: "ra"}),
		},
		{
			name: "items in another order",
			desired: replicationOf(ydbreplication.Item{Source: "a", Target: "ra"},
				ydbreplication.Item{Source: "b", Target: "rb"}),
			current: replicationOf(ydbreplication.Item{Source: "b", Target: "rb"},
				ydbreplication.Item{Source: "a", Target: "ra"}),
		},
		{
			name:    "a directory read back as its tables",
			desired: replicationOf(ydbreplication.Item{Source: "src", Target: "dst"}),
			current: replicationOf(ydbreplication.Item{Source: "src/t1", Target: "dst/t1"},
				ydbreplication.Item{Source: "src/sub/t2", Target: "dst/sub/t2"}),
		},
		{
			name:    "a replication that resolved no table yet",
			desired: replicationOf(ydbreplication.Item{Source: "a", Target: "ra"}),
			current: replicationOf(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.CompareReplication(test.desired, test.current), qt.DeepEquals,
				ydbreplication.ReplicationChanges{})
			c.Assert(ydbreplication.ReplicationsEqual(test.desired, test.current), qt.IsTrue)
		})
	}
}

// TestCompareReplication_Differs names what differs, and keeps apart the
// settings YDB changes in no replication.
func TestCompareReplication_Differs(t *testing.T) {
	base := replicationOf(ydbreplication.Item{Source: "a", Target: "ra"})
	with := func(change func(*ydbreplication.ReplicationSpec)) ydbreplication.ReplicationSpec {
		spec := base.Clone()
		change(&spec)
		return spec
	}
	tests := []struct {
		name    string
		desired ydbreplication.ReplicationSpec
		want    ydbreplication.ReplicationChanges
	}{
		{name: "another database", desired: with(func(s *ydbreplication.ReplicationSpec) {
			s.Connection.ConnectionString = "grpc://primary:2136/?database=/other"
		}), want: ydbreplication.ReplicationChanges{ConnectionString: true, CreateOnly: []string{"items"}}},
		{name: "another host", desired: with(func(s *ydbreplication.ReplicationSpec) {
			s.Connection.ConnectionString = "grpcs://standby:2135/?database=/prod"
		}), want: ydbreplication.ReplicationChanges{ConnectionString: true}},
		{name: "a credential", desired: with(func(s *ydbreplication.ReplicationSpec) { s.Connection.TokenSecretName = "t" }),
			want: ydbreplication.ReplicationChanges{Credentials: true}},
		{name: "another item", desired: with(func(s *ydbreplication.ReplicationSpec) {
			s.Items = append(s.Items, ydbreplication.Item{Source: "b", Target: "rb"})
		}), want: ydbreplication.ReplicationChanges{CreateOnly: []string{"items"}}},
		{name: "a target moved", desired: with(func(s *ydbreplication.ReplicationSpec) { s.Items[0].Target = "other" }),
			want: ydbreplication.ReplicationChanges{CreateOnly: []string{"items"}}},
		{name: "the global level", desired: with(func(s *ydbreplication.ReplicationSpec) { s.ConsistencyLevel = "global" }),
			want: ydbreplication.ReplicationChanges{CreateOnly: []string{"consistency_level"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.CompareReplication(test.desired, base), qt.DeepEquals, test.want)
		})
	}
}

// TestCompareReplication_CommitInterval tells two commit intervals apart by
// the milliseconds they denote.
func TestCompareReplication_CommitInterval(t *testing.T) {
	c := qt.New(t)
	global := func(interval string) ydbreplication.ReplicationSpec {
		spec := replicationOf(ydbreplication.Item{Source: "a", Target: "ra"})
		spec.ConsistencyLevel, spec.CommitInterval = "global", interval
		return spec
	}
	c.Assert(ydbreplication.CompareReplication(global("PT1M"), global("PT60S")).Any(), qt.IsFalse)
	c.Assert(ydbreplication.CompareReplication(global("PT1M"), global("PT30S")), qt.DeepEquals,
		ydbreplication.ReplicationChanges{CreateOnly: []string{"commit_interval"}})
}

// TestCompareTransfer_Equal holds two descriptions of one transfer equal where
// YDB keeps them alike: the batch defaults named and left out, a consumer YDB
// created where the declaration names none, and a lambda with surrounding
// space.
func TestCompareTransfer_Equal(t *testing.T) {
	c := qt.New(t)
	desired := ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: " ($m) -> { return []; }\n",
		BatchSizeBytes: 8 << 20, FlushInterval: "PT1M"}
	current := ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }",
		Consumer: "fbc17198-8229c5ec-37e45ba-fe47b6c1"}
	c.Assert(ydbreplication.CompareTransfer(desired, current), qt.DeepEquals, ydbreplication.TransferChanges{})
	c.Assert(ydbreplication.TransfersEqual(desired, current), qt.IsTrue)
}

// TestCompareTransfer_Differs names what differs, and keeps apart the
// settings YDB changes in no transfer.
func TestCompareTransfer_Differs(t *testing.T) {
	base := ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }", Consumer: "c"}
	with := func(change func(*ydbreplication.TransferSpec)) ydbreplication.TransferSpec {
		spec := base
		change(&spec)
		return spec
	}
	tests := []struct {
		name    string
		desired ydbreplication.TransferSpec
		want    ydbreplication.TransferChanges
	}{
		{name: "the lambda", desired: with(func(s *ydbreplication.TransferSpec) { s.Lambda = "($m) -> { return [1]; }" }),
			want: ydbreplication.TransferChanges{Lambda: true}},
		{name: "the batch size", desired: with(func(s *ydbreplication.TransferSpec) { s.BatchSizeBytes = 1024 }),
			want: ydbreplication.TransferChanges{Batch: true}},
		{name: "the flush interval", desired: with(func(s *ydbreplication.TransferSpec) { s.FlushInterval = "PT10S" }),
			want: ydbreplication.TransferChanges{Batch: true}},
		{name: "the source", desired: with(func(s *ydbreplication.TransferSpec) { s.Source = "other" }),
			want: ydbreplication.TransferChanges{CreateOnly: []string{"source"}}},
		{name: "the target", desired: with(func(s *ydbreplication.TransferSpec) { s.Target = "other" }),
			want: ydbreplication.TransferChanges{CreateOnly: []string{"target"}}},
		{name: "another consumer", desired: with(func(s *ydbreplication.TransferSpec) { s.Consumer = "d" }),
			want: ydbreplication.TransferChanges{CreateOnly: []string{"consumer"}}},
		{name: "a local source read through a connection", desired: with(func(s *ydbreplication.TransferSpec) {
			s.Connection.ConnectionString = "grpc://h:2136/?database=/local"
		}), want: ydbreplication.TransferChanges{ConnectionString: true, CreateOnly: []string{"source"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.CompareTransfer(test.desired, base), qt.DeepEquals, test.want)
		})
	}
}

// TestCompareTransfer_AnotherHost reads a remote transfer moved to another
// host of the same database as a connection change alone, since its source
// resolves to the same path there.
func TestCompareTransfer_AnotherHost(t *testing.T) {
	c := qt.New(t)
	current := ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }",
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"}}
	desired := current
	desired.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	desired.Source = "/prod/tp"
	c.Assert(ydbreplication.CompareTransfer(desired, current), qt.DeepEquals,
		ydbreplication.TransferChanges{ConnectionString: true})
}

// TestUnderTarget tells a table at a replica's path, or in a directory a
// replica target names, from one beside it.
func TestUnderTarget(t *testing.T) {
	c := qt.New(t)
	targets := []string{"replica", "orders_copy"}
	c.Assert(ydbreplication.UnderTarget("replica/accounts", targets), qt.IsTrue)
	c.Assert(ydbreplication.UnderTarget("orders_copy", targets), qt.IsTrue)
	c.Assert(ydbreplication.UnderTarget("replica_other", targets), qt.IsFalse)
	c.Assert(ydbreplication.UnderTarget("orders_copy2", targets), qt.IsFalse)
	c.Assert(ydbreplication.TablePath("app", "t"), qt.Equals, "app/t")
	c.Assert(ydbreplication.TablePath("", "t"), qt.Equals, "t")
}
