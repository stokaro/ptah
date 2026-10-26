package main

import (
	"encoding/json"
	"fmt"
	"os"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

func sourceLimitsEnabled() bool { return os.Getenv("FIXTURE_SOURCE_LIMITS") == "1" }

// limitEvidence is this fixture's closed interpretation of a retention header.
// Its JSON is the model knowledge reason, so export can recover both tokens.
type limitEvidence struct {
	Reason     string `json:"reason"`
	Provenance string `json:"provenance"`
}

func decodeLimits(request *protocol.DecodeSourceRequest, result *protocol.DecodedSource) (*protocol.DecodedSource, error) {
	if !sourceLimitsEnabled() {
		return result, nil
	}
	for i, document := range request.GetDocuments() {
		for _, limit := range document.GetLimits() {
			if limit.GetKind() != "retention" || limit.GetName() != "" {
				return nil, fmt.Errorf("fixture limits name the whole retention model")
			}
			evidence, err := json.Marshal(limitEvidence{Reason: limit.GetReason(), Provenance: limit.GetProvenance()})
			if err != nil {
				return nil, err
			}
			for _, kind := range result.Documents[i].GetCoverage().GetKinds() {
				if kind.GetModel().GetKind() == retentionKind {
					kind.Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: string(evidence)}
				}
			}
		}
	}
	return result, nil
}

func encodeLimits(request *protocol.EncodeSourceRequest, result *protocol.EncodedSource) (*protocol.EncodedSource, error) {
	if !sourceLimitsEnabled() {
		return encodeGroups(result), nil
	}
	for _, kind := range request.GetCoverage().GetKinds() {
		if len(kind.GetRegions()) != 0 {
			return nil, fmt.Errorf("fixture limits cannot encode namespace regions")
		}
		if kind.GetKnowledge().GetState() == protocol.ModelKnowledge_COMPLETE {
			continue
		}
		if kind.GetModel().GetKind() != retentionKind || kind.GetKnowledge().GetState() != protocol.ModelKnowledge_UNINSPECTED {
			return nil, fmt.Errorf("fixture limits cannot encode this knowledge")
		}
		var evidence limitEvidence
		if err := json.Unmarshal([]byte(kind.GetKnowledge().GetReason()), &evidence); err != nil {
			return nil, err
		}
		result.Limits = append(result.Limits, &protocol.SchemaCoverageLimit{Kind: "retention", Reason: evidence.Reason, Provenance: evidence.Provenance})
	}
	for _, subject := range request.GetCoverage().GetSubjects() {
		if state := subject.GetKnowledge().GetState(); state != protocol.ModelKnowledge_COMPLETE && state != protocol.ModelKnowledge_DEFAULTED {
			return nil, fmt.Errorf("fixture limits cannot encode subject knowledge")
		}
	}
	if os.Getenv("FIXTURE_SOURCE_LIMITS_DROP") == "1" {
		result.Limits = nil
	}
	return encodeGroups(result), nil
}
