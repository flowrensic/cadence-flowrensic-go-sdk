// Package engine is what the calls of this SDK say about it and about
// Cadence, and how it reads the history of a run from Cadence.
package engine

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/thriftrw/protocol"

	"go.flowrensic.com/cadence-flowrensic-go-sdk/internal/transport"
	"go.flowrensic.com/cadence-flowrensic-go-sdk/internal/worker"
)

// CadenceSdkHeader carries the version of the Cadence Go client.
const CadenceSdkHeader = "Flowrensic-Cadence-Sdk"

const modulePath = "go.flowrensic.com/cadence-flowrensic-go-sdk"

const cadenceModulePath = "go.uber.org/cadence"

// SdkHeaderValue is go, a slash and the version of this SDK: the tag of the
// module in the build info of the program, dev when it has none.
var SdkHeaderValue = "go/" + transport.SdkVersion(modulePath)

// CadenceSdkHeaderValue is the version of go.uber.org/cadence in the build
// info of the program, with no leading v, empty when it is not known.
var CadenceSdkHeaderValue = cadenceVersion()

func cadenceVersion() string {
	v := transport.ModuleVersion(cadenceModulePath)
	if v == "(devel)" {
		return ""
	}
	return v
}

// Tag is what every call of this SDK carries about it and about Cadence.
func Tag() transport.Tag {
	return transport.Tag{Sdk: SdkHeaderValue, EngineHeader: CadenceSdkHeader, EngineValue: CadenceSdkHeaderValue}
}

// callTimeout is how long one call to Cadence may take; the gRPC transport
// refuses a call with no deadline.
const callTimeout = 10 * time.Second

// pageSize is how many events one history call asks for.
const pageSize = 1000

// Histories reads the history of a run from Cadence for the core's
// telemetry: the whole history, as the Thrift binary encoding of
// shared.History, which the Flowrensic for Cadence server reads.
type Histories struct {
	Service workflowserviceclient.Interface
	Domain  string
}

var _ worker.Engine = (*Histories)(nil)

// closing is the event types that end a run.
var closing = map[shared.EventType]bool{
	shared.EventTypeWorkflowExecutionCompleted:      true,
	shared.EventTypeWorkflowExecutionFailed:         true,
	shared.EventTypeWorkflowExecutionTimedOut:       true,
	shared.EventTypeWorkflowExecutionCanceled:       true,
	shared.EventTypeWorkflowExecutionTerminated:     true,
	shared.EventTypeWorkflowExecutionContinuedAsNew: true,
}

// ReadHistory reads the history of one run. With no more events than
// sentEvents it returns changed false.
func (h *Histories) ReadHistory(ctx context.Context, workflowID, runID string, sentEvents int64) (worker.History, bool, error) {
	if h.Service == nil {
		return worker.History{}, false, nil
	}
	exec := &shared.WorkflowExecution{WorkflowId: &workflowID, RunId: &runID}
	var events []*shared.HistoryEvent
	var token []byte
	size := int32(pageSize)
	for {
		call, cancel := context.WithTimeout(ctx, callTimeout)
		res, err := h.Service.GetWorkflowExecutionHistory(call, &shared.GetWorkflowExecutionHistoryRequest{
			Domain: &h.Domain, Execution: exec, MaximumPageSize: &size, NextPageToken: token,
		})
		cancel()
		if err != nil {
			return worker.History{}, false, fmt.Errorf("flowrensic: history of %s/%s: %w", workflowID, runID, err)
		}
		events = append(events, res.GetHistory().GetEvents()...)
		if token = res.NextPageToken; len(token) == 0 {
			break
		}
	}
	if int64(len(events)) <= sentEvents {
		return worker.History{}, false, nil
	}
	b, err := Encode(&shared.History{Events: events})
	if err != nil {
		return worker.History{}, false, err
	}
	out := worker.History{Bytes: b, Events: int64(len(events)), Closed: closing[events[len(events)-1].GetEventType()], Namespace: h.Domain}
	if !out.Closed {
		call, cancel := context.WithTimeout(ctx, callTimeout)
		d, err := h.Service.DescribeWorkflowExecution(call, &shared.DescribeWorkflowExecutionRequest{Domain: &h.Domain, Execution: exec})
		cancel()
		if err == nil && d.PendingDecision != nil {
			out.TaskAttempt = int32(d.PendingDecision.GetAttempt()) + 1
		}
	}
	return out, true, nil
}

// Close closes nothing: the service is the worker's own.
func (h *Histories) Close() {}

// Encode is a history in the Thrift binary encoding.
func Encode(h *shared.History) ([]byte, error) {
	v, err := h.ToWire()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := protocol.Binary.Encode(v, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
