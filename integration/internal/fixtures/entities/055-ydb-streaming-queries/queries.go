// Package entities declares a stopped streaming query for schema round trips.
package entities

// StreamingObjects holds the continuous topic-to-topic query.
//
//ptah:schema:streamingquery name="stream_copy" run="false" text="INSERT INTO stream_output SELECT * FROM stream_input;"
type StreamingObjects struct{}
