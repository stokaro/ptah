package nodedispatch_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/internal/nodedispatch"
)

// Every renderer but YDB's refuses a topic node by the topics key, naming the
// node it was handed and the renderer.
func TestRefuseTopic(t *testing.T) {
	tests := []struct {
		node ast.Node
		want string
	}{
		{node: ast.NewCreateTopic("events", ast.TopicSpec{}), want: "topic events: the mysql renderer writes no topic; a topic needs target capability topics, which only YDB has"},
		{node: &ast.AlterTopicNode{Name: "events"}, want: "ALTER TOPIC events: the mysql renderer writes no topic; a topic needs target capability topics, which only YDB has"},
		{node: ast.NewAddTopicConsumer("events", ast.TopicConsumerSpec{Name: "worker"}), want: "ALTER TOPIC events ADD CONSUMER worker: the mysql renderer writes no topic; a topic needs target capability topics, which only YDB has"},
		{node: ast.NewDropTopic("events"), want: "DROP TOPIC events: the mysql renderer writes no topic; a topic needs target capability topics, which only YDB has"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			err := nodedispatch.RefuseTopic("mysql", test.node)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}
