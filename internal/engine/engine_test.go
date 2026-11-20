package engine

import (
	"bytes"
	"strings"
	"testing"

	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/wire"
)

func TestTheHistoryHook(t *testing.T) {
	t.Run("CFP-1-C13 a history is sent as the Thrift binary encoding of shared.History", func(t *testing.T) {
		id, ts, typ := int64(1), int64(42), shared.EventTypeWorkflowExecutionCompleted
		b, err := Encode(&shared.History{Events: []*shared.HistoryEvent{{EventId: &id, Timestamp: &ts, EventType: &typ}}})
		if err != nil {
			t.Fatal(err)
		}
		v, err := protocol.Binary.Decode(bytes.NewReader(b), wire.TStruct)
		if err != nil {
			t.Fatal(err)
		}
		var back shared.History
		if err := back.FromWire(v); err != nil || len(back.Events) != 1 || back.Events[0].GetEventType() != typ {
			t.Errorf("back %v %v", back.Events, err)
		}
	})
	t.Run("CFP-1-C13 every call names the SDK and the Cadence client", func(t *testing.T) {
		tag := Tag()
		if !strings.HasPrefix(tag.Sdk, "go/") || tag.EngineHeader != "Flowrensic-Cadence-Sdk" {
			t.Errorf("tag %+v", tag)
		}
	})
}
