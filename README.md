# Flowrensic for Cadence Go SDK

Flowrensic runs beside your durable workflow engine and finds problems in your workflows. Flowrensic for Cadence finds problems in Cadence workflows. Its server has 20 finding classes ported from Temporal and one of its own, Activity retrying, and a findings page with filters for severity, class, domain, workflow type and status.

This is the Go SDK. It connects a Cadence Go worker to your Flowrensic for Cadence server.

Quickstart: https://cadence.flowrensic.com/docs/quickstart/

## What it does

- Connects the worker to the server: call client.Connect from go.flowrensic.com/cadence-flowrensic-go-sdk/client with ServerBaseURL, WorkerName, Service and Domain. Call Shutdown on the client when the worker stops.
- Records each activity attempt: wrap each activity function with activity.Wrap from go.flowrensic.com/cadence-flowrensic-go-sdk/activity. The Cadence Go client has no activity interceptor, so the SDK wraps the function itself. It never changes what the activity returns.
- Sends the history of each run the worker saw, read with the Cadence service client you pass as Service.
- Your worker does not wait on Flowrensic. Recording an attempt never waits, and each call to Flowrensic stops after 5 seconds. When Flowrensic does not answer at start, the SDK asks again in the background.

## Install

```sh
go get go.flowrensic.com/cadence-flowrensic-go-sdk
```

Built and tested with the Cadence Go client go.uber.org/cadence v1.4.0. It needs Go 1.26.3 or later.

## Use

```go
import (
	"context"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"

	flowrensic "go.flowrensic.com/cadence-flowrensic-go-sdk/activity"
	"go.flowrensic.com/cadence-flowrensic-go-sdk/client"
)

fc, err := client.Connect(ctx, client.Options{
	ServerBaseURL: "http://localhost:8080",
	WorkerName:    "payments",
	Service:       service,
	Domain:        "sample",
})
if err != nil {
	return err
}
defer fc.Shutdown(context.Background())

w := worker.New(service, "sample", "payments", worker.Options{})
w.RegisterActivityWithOptions(flowrensic.Wrap(fc.Sink(), Charge, flowrensic.Options{}),
	activity.RegisterOptions{Name: "Charge"})
```

service is the Cadence service client your worker already has. Wrap takes a function whose first parameter is a context.Context and whose last result is an error.

## Learn more

- Quickstart: https://cadence.flowrensic.com/docs/quickstart/
- Packages: https://cadence.flowrensic.com/docs/packages/
