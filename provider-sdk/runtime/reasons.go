package runtime

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// malformedReason stands in for a reason provider.ValidateDiagnostic refuses,
// and for more than one reason in one status. Each end of the transport
// writes it, so a host sees the same sentence whether the SDK server or a
// server outside the SDK sent the reason.
const malformedReason = "the provider's reason is malformed and not shown"

// ErrorWithReason returns the error a service returns to fail a call with
// code and a reason the provider wrote for the host to show:
//
//	return nil, runtime.ErrorWithReason(codes.FailedPrecondition, "the streaming query capability was not established")
//
// The reason follows the rules of provider.ErrorReasonsFeature: the provider
// writes it, and it never quotes statement text, a value, an address or a
// credential, so it is never the text of an error the provider received. It
// is also the status message. The SDK server sends the reason only when the
// provider and the host both list the feature, and sends a sentence of its
// own in place of a reason provider.ValidateDiagnostic refuses. As with
// status.Error, codes.OK returns nil.
func ErrorWithReason(code codes.Code, reason string) error {
	reasoned, err := status.New(code, reason).WithDetails(&protocol.ProviderDiagnostic{Reason: reason})
	if err != nil {
		// WithDetails fails only for codes.OK, for which status.Error returns
		// nil, or for a message it cannot marshal, which a ProviderDiagnostic
		// is not.
		return status.Error(code, reason)
	}
	return reasoned.Err()
}

// remoteError turns a failed call into the host's error. A cancellation and
// a deadline become the context's errors. Any other code reads "provider
// <operation> failed (<code>)", followed by the provider's reason when the
// negotiated description lists provider.ErrorReasonsFeature and the status
// carries one. The status message is never used: it is whatever the
// provider's error chain wrote, and can quote a credential or a value.
func remoteError(ctx context.Context, description *protocol.Description, operation string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	code := status.Code(err)
	if code == codes.Canceled {
		return context.Canceled
	}
	if code == codes.DeadlineExceeded {
		return context.DeadlineExceeded
	}
	failure := fmt.Sprintf("provider %s failed (%s)", operation, code)
	if !slices.Contains(description.GetFeatures(), provider.ErrorReasonsFeature) {
		return errors.New(failure)
	}
	reason, given := statusReason(err)
	if !given {
		return errors.New(failure)
	}
	return fmt.Errorf("%s: %s", failure, reason)
}

// statusReason reads the reason a status carries: the one diagnostic in its
// details, or malformedReason for an invalid one or for more than one.
func statusReason(err error) (string, bool) {
	found, _, ok := diagnosticDetails(err)
	if !ok || len(found) == 0 {
		return "", false
	}
	if len(found) != 1 || provider.ValidateDiagnostic(found[0]) != nil {
		return malformedReason, true
	}
	return found[0].GetReason(), true
}

// diagnosticDetails splits a status's details into its diagnostics and the
// rest. A detail of the diagnostic type that does not unmarshal is reported
// as an empty diagnostic, which ValidateDiagnostic refuses. ok is false for
// an error that is not a status.
func diagnosticDetails(err error) (found []*protocol.ProviderDiagnostic, others []*anypb.Any, ok bool) {
	reported, ok := status.FromError(err)
	if !ok {
		return nil, nil, false
	}
	for _, detail := range reported.Proto().GetDetails() {
		if !detail.MessageIs(&protocol.ProviderDiagnostic{}) {
			others = append(others, detail)
			continue
		}
		diagnostic := &protocol.ProviderDiagnostic{}
		if detail.UnmarshalTo(diagnostic) != nil {
			diagnostic = &protocol.ProviderDiagnostic{}
		}
		found = append(found, diagnostic)
	}
	return found, others, true
}

// reasons is the server half of provider.ErrorReasonsFeature, run on the
// error of every call. Without the feature negotiated it removes every
// diagnostic, so a provider sets one without reading HostFeatures. With it,
// it keeps one valid diagnostic and puts malformedReason in place of an
// invalid one or of several. The status's code, message and other details
// are kept.
func (s *serverAdapter) reasons(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	reply, err := handler(ctx, request)
	if err == nil {
		return reply, nil
	}
	return reply, s.withReasons(err)
}

func (s *serverAdapter) withReasons(err error) error {
	found, others, ok := diagnosticDetails(err)
	if !ok || len(found) == 0 {
		return err
	}
	reported := proto.CloneOf(status.Convert(err).Proto())
	reported.Details = others
	if s.negotiatesReasons() {
		diagnostic := found[0]
		if len(found) != 1 || provider.ValidateDiagnostic(diagnostic) != nil {
			diagnostic = &protocol.ProviderDiagnostic{Reason: malformedReason}
		}
		detail, marshalErr := anypb.New(diagnostic)
		if marshalErr != nil {
			return status.FromProto(reported).Err()
		}
		reported.Details = append(reported.Details, detail)
	}
	return status.FromProto(reported).Err()
}

// negotiatesReasons reports whether the provider and the host both list
// provider.ErrorReasonsFeature.
func (s *serverAdapter) negotiatesReasons() bool {
	return slices.Contains(s.negotiated().GetFeatures(), provider.ErrorReasonsFeature)
}

// negotiatedDiagnostic returns the diagnostic a result may carry across the
// transport: none when the description does not list
// provider.ErrorReasonsFeature, malformedReason in place of one
// provider.ValidateDiagnostic refuses, and diagnostic itself otherwise. Unlike
// a lock receipt, an execution or transaction result with a malformed
// diagnostic is not refused: refusing it would turn a confirmed outcome into
// an unknown one, and only the text is at fault.
func negotiatedDiagnostic(description *protocol.Description, diagnostic *protocol.ProviderDiagnostic) *protocol.ProviderDiagnostic {
	if diagnostic == nil || !slices.Contains(description.GetFeatures(), provider.ErrorReasonsFeature) {
		return nil
	}
	if provider.ValidateDiagnostic(diagnostic) != nil {
		return &protocol.ProviderDiagnostic{Reason: malformedReason}
	}
	return diagnostic
}

// negotiatedExecution returns result with the diagnostic it may carry: the
// negotiated one, and none beside APPLIED, which no reason accompanies. Both
// ends of Execute apply it, so either one alone keeps an unnegotiated or a
// malformed reason out of the host's error. result itself is never changed.
func negotiatedExecution(description *protocol.Description, result *protocol.Execution) *protocol.Execution {
	diagnostic := negotiatedDiagnostic(description, result.GetDiagnostic())
	if result.GetOutcome() == protocol.Execution_APPLIED {
		diagnostic = nil
	}
	if diagnostic == result.GetDiagnostic() {
		return result
	}
	result = proto.CloneOf(result)
	result.Diagnostic = diagnostic
	return result
}

// negotiatedTransaction is negotiatedExecution for the result of a finish
// request with action. It reads success from provider.CheckTransactionResult,
// so the outcomes a reason may not accompany are the ones the check accepts.
func negotiatedTransaction(description *protocol.Description, action protocol.FinishTransactionRequest_Action,
	result *protocol.TransactionResult,
) *protocol.TransactionResult {
	diagnostic := negotiatedDiagnostic(description, result.GetDiagnostic())
	if provider.CheckTransactionResult(action, result) == nil {
		diagnostic = nil
	}
	if diagnostic == result.GetDiagnostic() {
		return result
	}
	result = proto.CloneOf(result)
	result.Diagnostic = diagnostic
	return result
}

// negotiatedReceipt returns a lock receipt without its diagnostic when the
// feature was not negotiated, and the receipt itself otherwise.
func negotiatedReceipt(description *protocol.Description, result *protocol.LockResult) *protocol.LockResult {
	if result.GetDiagnostic() == nil || slices.Contains(description.GetFeatures(), provider.ErrorReasonsFeature) {
		return result
	}
	result = proto.CloneOf(result)
	result.Diagnostic = nil
	return result
}
