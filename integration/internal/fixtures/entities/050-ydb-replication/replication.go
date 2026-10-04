package entities

// Orders is a table whose changes a changefeed streams, and the topic the
// transfer below reads.
//
//ptah:schema:table name="orders"
//ptah:schema:changefeed name="feed" mode="NEW_IMAGE" format="JSON"
type Orders struct {
	//ptah:schema:field name="id" type="BIGINT" primary not_null
	ID int64
	//ptah:schema:field name="total" type="BIGINT"
	Total int64
}

// OrderLog is the table the transfer writes.
//
//ptah:schema:table name="order_log"
type OrderLog struct {
	//ptah:schema:field name="partition" type="INT" primary not_null platform.ydb.type="Uint32"
	Partition uint32
	//ptah:schema:field name="offset" type="BIGINT" primary not_null platform.ydb.type="Uint64"
	Offset uint64
	//ptah:schema:field name="message" type="TEXT"
	Message string
}

// Mirror copies the accounts tables of the primary database.
//
//ptah:schema:async_replication name="mirror" connection_string="grpcs://primary.example.com:2135/?database=/prod" user="replicator" password_secret_path="secrets/replicator" consistency_level="global" commit_interval="PT30S"
//ptah:schema:async_replication:item replication="mirror" source="accounts" target="replica/accounts"
//ptah:schema:async_replication:item replication="mirror" source="/prod/ledger" target="replica/ledger"
type Mirror struct{}

// OrderTransfer writes each change of orders into order_log.
//
//ptah:schema:transfer name="order_transfer" source="orders/feed" target="order_log" using="($msg) -> { return [<| partition: $msg._partition, offset: $msg._offset, message: CAST($msg._data AS Utf8) |>]; }" batch_size_bytes="1048576" flush_interval="PT10S"
type OrderTransfer struct{}
