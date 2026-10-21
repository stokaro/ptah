//go:build integration

package integration_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
	"google.golang.org/protobuf/proto"
)

func metadataOptions(c *qt.C) transport.Options {
	c.Helper()
	binary := buildFixture(c, "metadata")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return transport.Options{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.MetadataFeature, provider.TransactionsFeature}}
}
func processMetadataScope(session *protocol.Session, kind protocol.MetadataScope_Kind) *protocol.MetadataScope {
	return &protocol.MetadataScope{SessionId: session.Id, Schema: "main", Table: fmt.Sprintf("metadata_%d", kind), Kind: kind}
}
func prepareMetadata(c *qt.C, client *transport.Client, scope *protocol.MetadataScope) {
	c.Helper()
	_, err := client.PrepareMetadata(c.Context(), &protocol.PrepareMetadataRequest{Scope: scope, RequestId: rand.Text()})
	c.Assert(err, qt.IsNil)
}
func putMetadataTag(c *qt.C, client *transport.Client, scope *protocol.MetadataScope, name string, version int64) {
	c.Helper()
	_, err := client.MutateMetadata(c.Context(), &protocol.MutateMetadataRequest{Scope: scope, RequestId: rand.Text(), Mutation: &protocol.MutateMetadataRequest_PutTag{PutTag: &protocol.MigrationTagRecord{Name: name, Version: version, RecordedAtSeconds: 1790000000}}})
	c.Assert(err, qt.IsNil)
}
func openMetadataReader(c *qt.C, client *transport.Client, scope *protocol.MetadataScope) *protocol.MetadataReadRequest {
	c.Helper()
	request := &protocol.MetadataReadRequest{Scope: proto.CloneOf(scope), ReaderId: rand.Text(), RequestId: rand.Text()}
	_, err := client.OpenMetadataRead(c.Context(), request)
	c.Assert(err, qt.IsNil)
	return request
}
func metadataPage(c *qt.C, client *transport.Client, reader *protocol.MetadataReadRequest, index uint64, limit uint32) *protocol.MetadataPage {
	c.Helper()
	reader = proto.CloneOf(reader)
	reader.RequestId = rand.Text()
	page, err := client.ReadMetadataPage(c.Context(), &protocol.ReadMetadataPageRequest{Reader: reader, Page: index, MaxRecords: limit})
	c.Assert(err, qt.IsNil)
	return page
}
func allMetadataTags(c *qt.C, client *transport.Client, reader *protocol.MetadataReadRequest) []*protocol.MigrationTagRecord {
	c.Helper()
	var records []*protocol.MigrationTagRecord
	for index := uint64(0); ; index++ {
		page := metadataPage(c, client, reader, index, 17)
		records = append(records, page.Tags...)
		if page.Complete {
			break
		}
	}
	_, err := client.CloseMetadataRead(c.Context(), reader)
	c.Assert(err, qt.IsNil)
	return records
}
func metadataEvent() *protocol.MigrationLogRecord {
	return &protocol.MigrationLogRecord{RunId: "run", Sequence: 1, Operation: "up", Version: 1, State: "started", Actor: "release-bot", ActorSource: "provided", AtSeconds: 1790000000}
}

func TestIndependentProviderMetadataTransactionAndDurableLog(t *testing.T) {
	c := qt.New(t)
	options := metadataOptions(c)
	for _, test := range []struct {
		name   string
		action protocol.FinishTransactionRequest_Action
		found  bool
	}{
		{"commit", protocol.FinishTransactionRequest_COMMIT, true}, {"rollback", protocol.FinishTransactionRequest_ROLLBACK, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			client := start(c, options)
			session := open(c, client, "")
			tags, logs := processMetadataScope(session, protocol.MetadataScope_TAGS), processMetadataScope(session, protocol.MetadataScope_LOG)
			prepareMetadata(c, client, tags)
			prepareMetadata(c, client, logs)
			tx, err := client.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{SessionId: session.Id, TransactionId: rand.Text(), Isolation: protocol.TransactionIsolation_DEFAULT})
			c.Assert(err, qt.IsNil)
			_, err = client.MutateMetadata(c.Context(), &protocol.MutateMetadataRequest{Scope: logs, RequestId: rand.Text(), Mutation: &protocol.MutateMetadataRequest_AppendLog{AppendLog: metadataEvent()}})
			c.Assert(err, qt.IsNil)
			bound := proto.CloneOf(tags)
			bound.TransactionId = tx.Id
			prepareMetadata(c, client, bound)
			putMetadataTag(c, client, bound, "release", 0)
			reader := openMetadataReader(c, client, bound)
			_, err = client.FinishTransaction(c.Context(), &protocol.FinishTransactionRequest{SessionId: session.Id, TransactionId: tx.Id, Action: test.action})
			c.Assert(err, qt.IsNil)
			_, err = client.ReadMetadataPage(c.Context(), &protocol.ReadMetadataPageRequest{Reader: reader, MaxRecords: 1})
			c.Assert(err, qt.IsNotNil)
			tag, err := client.LookupMetadataTag(c.Context(), &protocol.LookupMetadataTagRequest{Scope: tags, RequestId: rand.Text(), Name: "release"})
			c.Assert(err, qt.IsNil)
			c.Assert(tag.Found, qt.Equals, test.found)
			c.Assert(tag.GetRecord().GetVersion(), qt.Equals, int64(0))
			logReader := openMetadataReader(c, client, logs)
			page := metadataPage(c, client, logReader, 0, 1)
			c.Assert(page.Complete, qt.IsTrue)
			c.Assert(page.Logs, qt.HasLen, 1)
			c.Assert(proto.Equal(page.Logs[0], metadataEvent()), qt.IsTrue)
		})
	}
}
func TestIndependentProviderMetadataSnapshotsRemainImmutable(t *testing.T) {
	c := qt.New(t)
	client := start(c, metadataOptions(c))
	session := open(c, client, "")
	scope := processMetadataScope(session, protocol.MetadataScope_TAGS)
	absent := openMetadataReader(c, client, scope)
	page := metadataPage(c, client, absent, 0, 1)
	c.Assert(page.Exists, qt.IsFalse)
	c.Assert(page.Complete, qt.IsTrue)
	prepareMetadata(c, client, scope)
	for index := range provider.MaxMetadataRecords + 3 {
		putMetadataTag(c, client, scope, fmt.Sprintf("tag-%03d", index), int64(index))
	}
	reader := openMetadataReader(c, client, scope)
	putMetadataTag(c, client, scope, "new", 1000)
	putMetadataTag(c, client, scope, "tag-000", 999)
	tags := allMetadataTags(c, client, reader)
	c.Assert(tags, qt.HasLen, provider.MaxMetadataRecords+3)
	c.Assert(tags[0].Version, qt.Equals, int64(0))
	fresh := allMetadataTags(c, client, openMetadataReader(c, client, scope))
	c.Assert(fresh, qt.HasLen, provider.MaxMetadataRecords+4)
	c.Assert(fresh[0].Name, qt.Equals, "new")
	c.Assert(fresh[1].Version, qt.Equals, int64(999))
	deletion := &protocol.MutateMetadataRequest{Scope: scope, RequestId: rand.Text(), Mutation: &protocol.MutateMetadataRequest_DeleteTag{DeleteTag: "new"}}
	result, err := client.MutateMetadata(c.Context(), deletion)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Deleted, qt.IsTrue)
	deletion.RequestId = rand.Text()
	result, err = client.MutateMetadata(c.Context(), deletion)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Deleted, qt.IsFalse)
}
func TestIndependentProviderMetadataCloseFencesLateOpen(t *testing.T) {
	c := qt.New(t)
	client := start(c, metadataOptions(c))
	session := open(c, client, "")
	scope := processMetadataScope(session, protocol.MetadataScope_TAGS)
	reader := &protocol.MetadataReadRequest{Scope: scope, ReaderId: rand.Text(), RequestId: rand.Text()}
	_, err := client.CloseMetadataRead(c.Context(), reader)
	c.Assert(err, qt.IsNil)
	_, err = client.OpenMetadataRead(c.Context(), reader)
	c.Assert(err, qt.IsNotNil)
	live := openMetadataReader(c, client, scope)
	foreign := proto.CloneOf(live)
	foreign.Scope.Table = "other"
	_, err = client.CloseMetadataRead(c.Context(), foreign)
	c.Assert(err, qt.IsNotNil)
	page := metadataPage(c, client, live, 0, 1)
	c.Assert(page.Complete, qt.IsTrue)
	_, err = client.CloseMetadataRead(c.Context(), live)
	c.Assert(err, qt.IsNil)
	_, err = client.CloseMetadataRead(c.Context(), live)
	c.Assert(err, qt.IsNil)
}
func TestIndependentProviderMetadataUnknownWriteIsDurable(t *testing.T) {
	c := qt.New(t)
	options := metadataOptions(c)
	client := start(c, options)
	session := open(c, client, "lost-metadata-write")
	scope := processMetadataScope(session, protocol.MetadataScope_LOG)
	prepareMetadata(c, client, scope)
	_, err := client.MutateMetadata(c.Context(), &protocol.MutateMetadataRequest{Scope: scope, RequestId: rand.Text(), Mutation: &protocol.MutateMetadataRequest_AppendLog{AppendLog: metadataEvent()}})
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	client.Close()
	fresh := start(c, options)
	reopened := reopenLockedDatabase(c, fresh, session.Database, "")
	scope.SessionId = reopened.Id
	page := metadataPage(c, fresh, openMetadataReader(c, fresh, scope), 0, 5)
	c.Assert(page.Logs, qt.HasLen, 1)
	c.Assert(proto.Equal(page.Logs[0], metadataEvent()), qt.IsTrue)
}
func TestIndependentProviderMetadataLostOpenIsClosed(t *testing.T) {
	c := qt.New(t)
	client := start(c, metadataOptions(c))
	session := open(c, client, "lost-metadata-open")
	request := &protocol.MetadataReadRequest{Scope: processMetadataScope(session, protocol.MetadataScope_TAGS), ReaderId: rand.Text(), RequestId: rand.Text()}
	_, err := client.OpenMetadataRead(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	_, err = client.ReadMetadataPage(c.Context(), &protocol.ReadMetadataPageRequest{Reader: request, MaxRecords: 1})
	c.Assert(err, qt.IsNotNil)
	_, err = client.CloseMetadataRead(c.Context(), request)
	c.Assert(err, qt.IsNil)
}

func executeMetadataSQL(c *qt.C, client *transport.Client, session *protocol.Session, body string) {
	c.Helper()
	result, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session.Id, RequestId: rand.Text(), Units: []*protocol.Unit{{ChangeId: "metadata-layout", Body: &protocol.Unit_Sql{Sql: body}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(provider.CheckExecution(1, result), qt.IsNil)
}
func TestIndependentProviderMetadataRefusesForeignLayoutAndTriggers(t *testing.T) {
	c := qt.New(t)
	options := metadataOptions(c)
	t.Run("foreign layout", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		session := open(c, client, "")
		scope := processMetadataScope(session, protocol.MetadataScope_TAGS)
		executeMetadataSQL(c, client, session, `CREATE TABLE metadata_2 (private_value TEXT);`)
		_, err := client.PrepareMetadata(c.Context(), &protocol.PrepareMetadataRequest{Scope: scope, RequestId: rand.Text()})
		c.Assert(err, qt.IsNotNil)
		_, err = client.LookupMetadataTag(c.Context(), &protocol.LookupMetadataTagRequest{Scope: scope, RequestId: rand.Text(), Name: "release"})
		c.Assert(err, qt.IsNotNil)
	})
	t.Run("trigger", func(t *testing.T) {
		c := qt.New(t)
		client := start(c, options)
		session := open(c, client, "")
		scope := processMetadataScope(session, protocol.MetadataScope_TAGS)
		prepareMetadata(c, client, scope)
		executeMetadataSQL(c, client, session, `CREATE TRIGGER metadata_trigger AFTER INSERT ON metadata_2 BEGIN UPDATE metadata_2 SET version = 999; END;`)
		_, err := client.MutateMetadata(c.Context(), &protocol.MutateMetadataRequest{Scope: scope, RequestId: rand.Text(), Mutation: &protocol.MutateMetadataRequest_PutTag{PutTag: &protocol.MigrationTagRecord{Name: "release", Version: 1, RecordedAtSeconds: 1790000000}}})
		c.Assert(err, qt.IsNotNil)
	})
}
func TestIndependentProviderMetadataPageReplayClosesReader(t *testing.T) {
	c := qt.New(t)
	client := start(c, metadataOptions(c))
	session := open(c, client, "")
	scope := processMetadataScope(session, protocol.MetadataScope_TAGS)
	prepareMetadata(c, client, scope)
	putMetadataTag(c, client, scope, "first", 1)
	putMetadataTag(c, client, scope, "second", 2)
	reader := openMetadataReader(c, client, scope)
	first := metadataPage(c, client, reader, 0, 1)
	c.Assert(first.Complete, qt.IsFalse)
	_, err := client.ReadMetadataPage(c.Context(), &protocol.ReadMetadataPageRequest{Reader: reader, Page: 0, MaxRecords: 1})
	c.Assert(err, qt.IsNotNil)
	_, err = client.ReadMetadataPage(c.Context(), &protocol.ReadMetadataPageRequest{Reader: reader, Page: 1, MaxRecords: 1})
	c.Assert(err, qt.IsNotNil)
}

func TestIndependentProviderMetadataSessionCloseInvalidatesReaders(t *testing.T) {
	c := qt.New(t)
	client := start(c, metadataOptions(c))
	session := open(c, client, "")
	reader := openMetadataReader(c, client, processMetadataScope(session, protocol.MetadataScope_TAGS))
	c.Assert(client.CloseSession(c.Context(), session.Id), qt.IsNil)
	_, err := client.ReadMetadataPage(c.Context(), &protocol.ReadMetadataPageRequest{Reader: reader, MaxRecords: 1})
	c.Assert(err, qt.IsNotNil)
}
