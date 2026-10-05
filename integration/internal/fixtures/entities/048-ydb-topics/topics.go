package entities

// Events is the topic order events are written to, read by two consumers.
//
//ptah:schema:topic name="order_events" schema="app" min_active_partitions="2" retention_period="PT36H" supported_codecs="raw,gzip"
//ptah:schema:topic:consumer name="billing" topic="order_events" schema="app" important="true"
//ptah:schema:topic:consumer name="audit" topic="order_events" schema="app" read_from="2026-01-01T00:00:00Z"
type Events struct{}
