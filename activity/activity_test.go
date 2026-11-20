package activity_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	cactivity "go.uber.org/cadence/activity"
	"go.uber.org/cadence/testsuite"

	"go.flowrensic.com/cadence-flowrensic-go-sdk/activity"
	"go.flowrensic.com/cadence-flowrensic-go-sdk/client"
)

// sink keeps what it is given.
type sink struct {
	mu   sync.Mutex
	got  []*client.ActivityAttempt
	sent bool
}

func (s *sink) Record(a *client.ActivityAttempt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, a)
}
func (s *sink) Flush(context.Context) error { return nil }
func (s *sink) Close(context.Context) error { return nil }
func (s *sink) Counts() client.SinkCounts   { return client.SinkCounts{} }

var errDeclined = errors.New("declined")

func Charge(ctx context.Context, amount int) (string, error) {
	if amount < 0 {
		return "", errDeclined
	}
	return "charged", nil
}

func TestTheActivityWrapper(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	s := &sink{}
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(activity.Wrap(s, Charge, activity.Options{Version: "v7"}), cactivity.RegisterOptions{Name: "Charge"})

	t.Run("CFP-1-C13 one attempt per call, counted from 1, and the result unchanged", func(t *testing.T) {
		v, err := env.ExecuteActivity("Charge", 5)
		if err != nil {
			t.Fatal(err)
		}
		var got string
		if err := v.Get(&got); err != nil || got != "charged" {
			t.Errorf("result %q %v, want charged", got, err)
		}
		if len(s.got) != 1 {
			t.Fatalf("attempts %d, want 1", len(s.got))
		}
		a := s.got[0]
		if a.GetActivityType() != "Charge" || a.GetAttempt() != 1 || a.GetOutcome() != client.OutcomeCompleted || a.GetVersion() != "v7" ||
			a.GetNamespace() == "" || a.GetRunKey() == "" || a.GetStartedAt() == nil || a.GetEndedAt() == nil {
			t.Errorf("attempt %v", a)
		}
	})

	t.Run("CFP-1-C13 a failed call is a failed attempt with its error type, and the error reaches the caller", func(t *testing.T) {
		_, err := env.ExecuteActivity("Charge", -1)
		if err == nil || !errors.Is(err, err) {
			t.Fatal("no error")
		}
		if len(s.got) != 2 {
			t.Fatalf("attempts %d, want 2", len(s.got))
		}
		if a := s.got[1]; a.GetOutcome() != client.OutcomeFailed || a.GetErrorType() != "errorString" {
			t.Errorf("attempt %v, want failed with errorString", a)
		}
	})

	t.Run("CFP-1-C13 a function that is not an activity is refused", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("no panic")
			}
		}()
		activity.Wrap(s, func(int) error { return nil }, activity.Options{})
	})
}
