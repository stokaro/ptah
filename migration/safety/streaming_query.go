package safety

import (
	"ptah.run/internal/ydbstream"
)

const streamingExecutionChange = "ALTER STREAMING QUERY changes continuous query execution"

const streamingCheckpointLoss = "the operation removes or resets streaming-query checkpoint state; a rollback restores the declaration, not the discarded state"

func assessStreamingQuery(node *ydbstream.Node, assessment StatementAssessment) StatementAssessment {
	assessment.Subject = node.Name
	if node.Operation == ydbstream.DropOperation || (node.Operation == ydbstream.AlterOperation && !ydbstream.SameBody(node.Spec.Text, node.Previous.Text)) {
		assessment.Severity, assessment.Reason = Destructive, streamingCheckpointLoss
	} else if node.Operation == ydbstream.AlterOperation {
		assessment.Severity, assessment.Reason = Warning, streamingExecutionChange
	}
	return assessment
}
