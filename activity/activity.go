// Package activity wraps the activity functions of a Cadence Go worker so
// each attempt is recorded to the Flowrensic sink. The Cadence Go client has
// no activity interceptor, so the wrapper sits around the function itself.
package activity

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	cactivity "go.uber.org/cadence/activity"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.flowrensic.com/cadence-flowrensic-go-sdk/client"
)

// Options is what the worker says of itself on each attempt.
type Options struct {
	// Version is the version this worker serves. Empty, none.
	Version string
}

var (
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

// Wrap gives a function of the same type as fn that runs fn and records one
// attempt to sink. fn is an activity function: its first parameter is a
// context.Context and its last result an error. Register what Wrap gives, with
// the activity name of fn, such as
//
//	worker.RegisterActivityWithOptions(activity.Wrap(sink, Charge, activity.Options{}),
//	    activity.RegisterOptions{Name: "Charge"})
//
// The attempt never changes what fn returns, the error it fails with, or a
// panic.
func Wrap[F any](sink client.AttemptSink, fn F, options Options) F {
	v := reflect.ValueOf(fn)
	t := v.Type()
	if t.Kind() != reflect.Func || t.NumIn() == 0 || t.In(0) != contextType || t.NumOut() == 0 || t.Out(t.NumOut()-1) != errorType {
		panic(fmt.Sprintf("flowrensic: activity.Wrap takes a func(context.Context, ...) (..., error), not %s", t))
	}
	wrapped := reflect.MakeFunc(t, func(args []reflect.Value) (results []reflect.Value) {
		ctx, _ := args[0].Interface().(context.Context)
		info := cactivity.GetInfo(ctx)
		started := time.Now()
		defer func() {
			if r := recover(); r != nil {
				record(sink, info, started, client.OutcomeFailed, "PanicError", options)
				panic(r)
			}
		}()
		results = v.Call(args)
		outcome, kind := client.OutcomeCompleted, ""
		if err, _ := results[len(results)-1].Interface().(error); err != nil {
			outcome, kind = outcomeOf(ctx, err)
		}
		record(sink, info, started, outcome, kind, options)
		return results
	})
	return wrapped.Interface().(F)
}

// outcomeOf is how an attempt that returned err ended, and the type of its
// error: the reason of a custom error, else the Go type name.
func outcomeOf(ctx context.Context, err error) (client.Outcome, string) {
	if errors.Is(err, context.Canceled) || (ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled)) {
		return client.OutcomeCanceled, "Canceled"
	}
	var reasoned interface{ Reason() string }
	if errors.As(err, &reasoned) && reasoned.Reason() != "" {
		return client.OutcomeFailed, reasoned.Reason()
	}
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return client.OutcomeFailed, t.Name()
}

// record sends one attempt. Cadence counts attempts from 0; the attempt sent
// counts them from 1.
func record(sink client.AttemptSink, info cactivity.Info, started time.Time, outcome client.Outcome, kind string, options Options) {
	ended := time.Now()
	a := &client.ActivityAttempt{
		Namespace:           info.WorkflowDomain,
		WorkflowExecutionId: info.WorkflowExecution.ID,
		RunKey:              info.WorkflowExecution.RunID,
		ActivityType:        info.ActivityType.Name,
		ActivityId:          info.ActivityID,
		Attempt:             info.Attempt + 1,
		TaskQueue:           info.TaskList,
		Outcome:             outcome,
		ErrorType:           kind,
		StartedAt:           timestamppb.New(started),
		EndedAt:             timestamppb.New(ended),
		DurationMs:          ended.Sub(started).Milliseconds(),
		Version:             options.Version,
		HeartbeatTimeoutMs:  info.HeartbeatTimeout.Milliseconds(),
	}
	if info.WorkflowType != nil {
		a.WorkflowType = info.WorkflowType.Name
	}
	if !info.ScheduledTimestamp.IsZero() {
		a.ScheduledAt = timestamppb.New(info.ScheduledTimestamp)
	}
	sink.Record(a)
	if spender, ok := sink.(client.TimeSpender); ok {
		spender.Spent(time.Since(ended))
	}
}
