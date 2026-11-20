// Package client connects a Cadence Go worker to the Flowrensic for Cadence
// server: the hello and the worker telemetry, the sink the activity wrapper
// records to, and the reading of the histories of the runs the worker saw.
package client

import (
	"context"
	"net/http"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"

	"go.flowrensic.com/cadence-flowrensic-go-sdk/internal/engine"
	"go.flowrensic.com/cadence-flowrensic-go-sdk/internal/transport"
	"go.flowrensic.com/cadence-flowrensic-go-sdk/internal/worker"
)

// The headers every Flowrensic call carries.
const (
	CurrentEnvHeader   = transport.CurrentEnvHeader
	EnvAccessKeyHeader = transport.EnvAccessKeyHeader
	SdkHeader          = transport.SdkHeader
	WorkerIDHeader     = transport.WorkerIDHeader
	// CadenceSdkHeader carries the version of the Cadence Go client.
	CadenceSdkHeader = engine.CadenceSdkHeader
)

// SdkHeaderValue is go, a slash and the version of this SDK.
var SdkHeaderValue = engine.SdkHeaderValue

// CadenceSdkHeaderValue is the version of go.uber.org/cadence the program
// runs, empty when not known.
var CadenceSdkHeaderValue = engine.CadenceSdkHeaderValue

// The neutral types of the core, under the names of this SDK.
type (
	// ActivityAttempt is one attempt of one activity, as the sink sends it.
	ActivityAttempt = worker.ActivityAttempt
	// Outcome is how an attempt ended.
	Outcome = worker.Outcome
	// AttemptSink is where the activity wrapper records attempts.
	AttemptSink = worker.AttemptSink
	// BufferOptions is how the attempts wait and leave.
	BufferOptions = worker.BufferOptions
	// NoAnswerStrategy is what to do when the hello gets no answer.
	NoAnswerStrategy = worker.NoAnswerStrategy
	// Answer is what Flowrensic answered the hello with.
	Answer = worker.Answer
	// SinkCounts is what a sink sent, dropped and holds.
	SinkCounts = worker.SinkCounts
	// TimeSpender is told the time the SDK's own code took.
	TimeSpender = worker.TimeSpender
)

// The outcomes of an attempt.
const (
	OutcomeCompleted = worker.OutcomeCompleted
	OutcomeFailed    = worker.OutcomeFailed
	OutcomeCanceled  = worker.OutcomeCanceled
)

// Options is what a worker tells Flowrensic about itself, and where its
// telemetry reads and sends.
type Options struct {
	// ServerBaseURL is the Flowrensic for Cadence server, such as
	// http://flowrensic:8080.
	ServerBaseURL string
	// CurrentEnv is the env this worker runs in. Empty, none.
	CurrentEnv string
	// EnvAccessKey is the access key the server asks for. Empty, none.
	EnvAccessKey string
	// WorkerID names this worker. Empty, the host name and the process id.
	WorkerID string
	// WorkerName is the name of this worker, the same for every copy of it.
	WorkerName string
	// Version is the version this worker serves. Empty, none.
	Version string
	// Service is the worker's own Cadence service client, used to read the
	// histories of the runs it saw. It is not closed at shutdown. Nil, no
	// history is sent.
	Service workflowserviceclient.Interface
	// Domain is the Cadence domain the worker polls.
	Domain string
	// NoAnswer is what to do when the hello gets no answer. Zero, keep trying.
	NoAnswer NoAnswerStrategy
	// Timeout is how long one call to Flowrensic may take. Zero, 5 seconds.
	Timeout time.Duration
	// ShutdownTimeout is how long Shutdown may send histories. Zero, 20
	// seconds.
	ShutdownTimeout time.Duration
	// Buffer is how the attempts of the sink wait and leave.
	Buffer BufferOptions
	// HTTPClient sends the calls. Nil, http.DefaultClient.
	HTTPClient *http.Client
}

// Client is a worker's connection to the Flowrensic for Cadence server.
type Client struct {
	telemetry *worker.Telemetry
}

// Connect says hello to the server and starts the telemetry. With no answer
// it does what options.NoAnswer says. Call Shutdown when the worker stops.
func Connect(ctx context.Context, options Options) (*Client, error) {
	var hook worker.Engine
	if options.Service != nil {
		hook = &engine.Histories{Service: options.Service, Domain: options.Domain}
	}
	t, err := worker.Start(ctx, worker.Options{
		ServerBaseURL:   options.ServerBaseURL,
		CurrentEnv:      options.CurrentEnv,
		EnvAccessKey:    options.EnvAccessKey,
		WorkerID:        options.WorkerID,
		WorkerName:      options.WorkerName,
		Version:         options.Version,
		Engine:          hook,
		Tag:             engine.Tag(),
		NoAnswer:        options.NoAnswer,
		Timeout:         options.Timeout,
		ShutdownTimeout: options.ShutdownTimeout,
		Buffer:          options.Buffer,
		HTTPClient:      options.HTTPClient,
	})
	if err != nil {
		return nil, err
	}
	return &Client{telemetry: t}, nil
}

// Sink is the sink the activity wrapper records to. The runs of the
// attempts it takes are the runs whose histories the client sends.
func (c *Client) Sink() AttemptSink { return c.telemetry.Sink() }

// Answer is what the server answered the hello with.
func (c *Client) Answer() Answer { return c.telemetry.Answer() }

// WorkerID is the worker id the hello carried.
func (c *Client) WorkerID() string { return c.telemetry.WorkerID() }

// Shutdown sends what the sink holds and the histories not sent yet, then
// stops the telemetry.
func (c *Client) Shutdown(ctx context.Context) error { return c.telemetry.Shutdown(ctx) }

// KeepTrying, the default, asks again in the background, every delay.
func KeepTrying(delay time.Duration) NoAnswerStrategy { return worker.KeepTrying(delay) }

// Fail returns the error of the hello.
func Fail() NoAnswerStrategy { return worker.Fail() }

// GiveUpQuietly stops asking and sends nothing.
func GiveUpQuietly() NoAnswerStrategy { return worker.GiveUpQuietly() }
