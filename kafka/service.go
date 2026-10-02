package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	goguard "github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/obs"
)

// ServiceConfig describes a whole Kafka service in one place: where messages come from, where
// saved ones wait, and what to do with each topic. NewService builds the consumer and the
// redriver from it, connects them, and Service.Run runs them together.
//
// Every setting of the pieces underneath stays available. Consumer and Redriver take the same
// structs you would pass to NewConsumer and dlq.NewRedriver, and the service only fills in
// what it owns (the client, the store, the topic bindings, the handler and the breakers).
type ServiceConfig struct {
	// Client reads from Kafka. Give a ready Client, or NewClient to have the service create one
	// for the topics below (confluent.NewClientFunc builds one from a librdkafka config). With
	// NewClient the service also closes the client in Close.
	Client    Client
	NewClient func(topics []string) (Client, error)

	// Store holds the messages that could not be finished. Give a ready Store, or DataDir to open
	// a durable write-ahead log there (see dlq.OpenWAL), configured by WAL. When Secure is set the
	// log is wrapped by dlq.Secure, so what reaches disk is encrypted. With DataDir the service
	// also closes the store in Close.
	Store   dlq.Store
	DataDir string
	WAL     dlq.WALOptions
	Secure  *dlq.SecureOptions

	// Handlers has one function per topic (see HandlerConfig): the simple case. Pipelines has
	// topics with several steps (see package pipeline). A topic appears in exactly one of them.
	Handlers  []HandlerConfig
	Pipelines []Binding

	// Consumer and Redriver are the full configuration of the pieces underneath, for anything the
	// service does not set itself: Workers, backoff, MaxAttempts, rates, hooks and so on. Leave
	// them zero for the defaults. Consumer.Client, Consumer.Store and Consumer.Bindings, and
	// Redriver.Store, must be left empty: the service sets them. Redriver.Handler may be set to
	// replace the handler the service builds, and Redriver.Breakers adds to the ones it collects.
	Consumer Config
	Redriver dlq.RedriveConfig

	// Replay finishes saved work that did not come from Kafka, by the kind of its source: for
	// example "http" for requests a guarded HTTP client saved (goguard.Client.Replay builds the
	// handler). Saved Kafka messages always go to the pipeline of their own topic.
	Replay map[string]dlq.RedriveHandler

	// HTTP is a guarded client (goguard.NewClient) whose saved requests this service should finish.
	// It registers the client's replay handler for "http" records and lets the redriver watch the
	// client's circuits, unless Replay or Redriver.BreakerSource already say otherwise. Set
	// Replay["http"] = HTTP.Replay(prepare) yourself to add credentials when replaying.
	HTTP *goguard.Client

	// Events is given to the consumer, the redriver and the log the service opens, unless they
	// were given a sink of their own. It must not block; wrap a slow sink in obs.Dispatcher.
	Events obs.Sink
}

// Service is a consumer and a redriver working together on one store.
type Service struct {
	consumer *Consumer
	redriver *dlq.Redriver
	store    dlq.Store

	ownsStore   bool
	ownedCloser io.Closer // the client, when the service created it

	mu      sync.Mutex
	started bool
	closed  bool
}

// NewService validates cfg and builds everything. It opens the log and the client if you asked it
// to; if it fails after that it closes what it opened.
func NewService(cfg ServiceConfig) (s *Service, err error) {
	if (cfg.Client == nil) == (cfg.NewClient == nil) {
		return nil, errors.New("kafka: ServiceConfig needs exactly one of Client and NewClient")
	}
	if (cfg.Store == nil) == (cfg.DataDir == "") {
		return nil, errors.New("kafka: ServiceConfig needs exactly one of Store and DataDir")
	}
	if cfg.Consumer.Client != nil || cfg.Consumer.Store != nil || len(cfg.Consumer.Bindings) > 0 {
		return nil, errors.New("kafka: set Client, Store, Handlers and Pipelines on ServiceConfig, not on Consumer")
	}
	if cfg.Redriver.Store != nil {
		return nil, errors.New("kafka: set the store on ServiceConfig, not on Redriver")
	}

	bindings := append([]Binding(nil), cfg.Pipelines...)
	for _, h := range cfg.Handlers {
		b, herr := HandlerBinding(h)
		if herr != nil {
			return nil, herr
		}
		bindings = append(bindings, b)
	}
	if len(bindings) == 0 {
		return nil, errors.New("kafka: ServiceConfig needs at least one handler or pipeline")
	}
	rd, err := RedriveFor(bindings) // also rejects a topic that appears twice
	if err != nil {
		return nil, err
	}

	s = &Service{}
	defer func() {
		if err != nil {
			_ = s.Close()
			s = nil
		}
	}()

	if cfg.Store != nil {
		s.store = cfg.Store
	} else {
		wal := cfg.WAL
		if wal.Events == nil {
			wal.Events = cfg.Events
		}
		w, werr := dlq.OpenWAL(cfg.DataDir, wal)
		if werr != nil {
			return nil, fmt.Errorf("kafka: opening the dead-letter log: %w", werr)
		}
		s.store, s.ownsStore = w, true
		if cfg.Secure != nil {
			sealed, serr := dlq.Secure(w, *cfg.Secure)
			if serr != nil {
				return nil, serr // the deferred Close closes the log that was opened
			}
			s.store = sealed
		}
	}

	client := cfg.Client
	if client == nil {
		topics := make([]string, len(bindings))
		for i, b := range bindings {
			topics[i] = b.Topic
		}
		if client, err = cfg.NewClient(topics); err != nil {
			return nil, fmt.Errorf("kafka: creating the client: %w", err)
		}
		if c, ok := client.(io.Closer); ok {
			s.ownedCloser = c
		}
	}

	cc := cfg.Consumer
	cc.Client, cc.Store, cc.Bindings = client, s.store, bindings
	if cc.Events == nil {
		cc.Events = cfg.Events
	}
	if s.consumer, err = NewConsumer(cc); err != nil {
		return nil, err
	}

	replay := cfg.Replay
	rc := cfg.Redriver
	if cfg.HTTP != nil {
		if _, set := replay["http"]; !set {
			replay = make(map[string]dlq.RedriveHandler, len(cfg.Replay)+1)
			for k, v := range cfg.Replay {
				replay[k] = v
			}
			replay["http"] = cfg.HTTP.Replay(nil)
		}
		if rc.BreakerSource == nil {
			rc.BreakerSource = cfg.HTTP.Breakers
		}
	}
	rc.Store = s.store
	if rc.Handler == nil {
		rc.Handler = replayHandler(rd.Handler, replay)
	}
	rc.Breakers = mergeBreakers(rd, cfg.Redriver.Breakers)
	if rc.Events == nil {
		rc.Events = cfg.Events
	}
	if s.redriver, err = dlq.NewRedriver(rc); err != nil {
		return nil, err
	}
	return s, nil
}

// replayHandler sends a saved record to the handler for its kind of source: Kafka messages to the
// pipeline of their topic, anything else to the handler registered for that kind.
func replayHandler(kafkaHandler dlq.RedriveHandler, other map[string]dlq.RedriveHandler) dlq.RedriveHandler {
	if len(other) == 0 {
		return kafkaHandler
	}
	return func(ctx context.Context, it *dlq.Item) error {
		if h, ok := other[it.Record.Source.Kind]; ok {
			return h(ctx, it)
		}
		return kafkaHandler(ctx, it)
	}
}

func mergeBreakers(rd Redrive, extra map[string]*breaker.Breaker) map[string]*breaker.Breaker {
	out := make(map[string]*breaker.Breaker, len(rd.Breakers)+len(extra))
	for k, v := range rd.Breakers {
		out[k] = v
	}
	for k, v := range extra { // the caller's entries win
		out[k] = v
	}
	return out
}

// Run runs the consumer and the redriver until ctx ends or either stops with an error. When one
// stops the other is stopped too, and the errors are returned together. It returns nil when ctx
// ended normally. Run may be called once.
func (s *Service) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("kafka: the service is closed")
	}
	if s.started {
		s.mu.Unlock()
		return errors.New("kafka: Run was already called")
	}
	s.started = true
	s.mu.Unlock()

	ctx, stop := context.WithCancel(ctx)
	defer stop()
	errs := make(chan error, 2)
	go func() { errs <- s.redriver.Run(ctx) }()
	go func() { errs <- s.consumer.Run(ctx) }()
	first := <-errs
	stop()
	second := <-errs
	var out []error
	for _, e := range []error{first, second} {
		if e != nil && !errors.Is(e, context.Canceled) {
			out = append(out, e)
		}
	}
	return errors.Join(out...)
}

// Consumer returns the consumer, for Stats and the like.
func (s *Service) Consumer() *Consumer { return s.consumer }

// Redriver returns the redriver, for Stats, Wake and the like.
func (s *Service) Redriver() *dlq.Redriver { return s.redriver }

// Store returns the store the service uses, for listing parked messages and so on.
func (s *Service) Store() dlq.Store { return s.store }

// Close releases what the service opened itself: the client made by NewClient and the log opened
// from DataDir. Call it after Run has returned. A client or a store you passed in is yours to
// close. It is safe to call more than once.
func (s *Service) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	var errs []error
	if s.ownedCloser != nil {
		errs = append(errs, s.ownedCloser.Close())
	}
	if s.ownsStore && s.store != nil {
		errs = append(errs, s.store.Close())
	}
	return errors.Join(errs...)
}
