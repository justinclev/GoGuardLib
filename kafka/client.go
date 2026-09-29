// Package kafka is a Kafka consumer that keeps working when the things it calls
// are down, without losing messages.
//
// The Consumer reads messages, runs each through a pipeline (see package
// pipeline), and commits an offset only when the message is safe: fully
// processed, or durably stored in a dead-letter store together with its progress.
// While a dependency's circuit breaker is open it pauses the topic and leaves the
// messages in Kafka, which is a far bigger buffer than any local queue. If a
// message cannot be processed or stored it is not skipped: the consumer seeks
// back to it and retries after a backoff.
//
// This package does not import a Kafka client library. It talks to the small
// Client interface; package confluent adapts confluent-kafka-go to it, and tests
// can use a simulated broker.
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
)

// TopicPartition identifies a partition.
type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string { return fmt.Sprintf("%s[%d]", tp.Topic, tp.Partition) }

// TopicPartitionOffset is a position in a partition. For a commit, Offset is the
// offset of the NEXT message to read (last processed plus one).
type TopicPartitionOffset struct {
	TopicPartition
	Offset int64
}

// Message is a consumed Kafka message, independent of any client library.
type Message struct {
	TopicPartition
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []dlq.Header
	Timestamp time.Time
}

// Event is what Client.Poll returns: a Message, or nil when the timeout passed
// with nothing to report. Rebalances are not events; see RebalanceHandler.
type Event any

// RebalanceHandler is told about group rebalances. The Client calls it
// synchronously from inside Poll, on the polling goroutine, and completes the
// assignment or revocation only after the handler returns. That ordering is what
// lets the consumer commit its progress for a partition before giving it up.
type RebalanceHandler interface {
	// OnAssigned is called when the group gives this consumer partitions.
	OnAssigned(parts []TopicPartition) error
	// OnRevoked is called when partitions are about to be taken away. Commit
	// progress for them before returning.
	OnRevoked(parts []TopicPartition) error
}

// Client is the part of a Kafka consumer that this package needs. Implementations
// must be configured with auto-commit and automatic offset storing OFF: this
// package decides when an offset is safe to commit.
type Client interface {
	// Poll waits up to timeout for the next message. It returns (nil, nil) on
	// timeout. Rebalance callbacks run inside Poll. A returned error is fatal to
	// the consumer, including an error returned by the RebalanceHandler.
	Poll(ctx context.Context, timeout time.Duration) (Event, error)
	// SetRebalanceHandler registers the handler for rebalances. It is called once,
	// before the first Poll.
	SetRebalanceHandler(h RebalanceHandler)
	// Pause stops fetching from the partitions. The client must keep being polled.
	Pause(parts []TopicPartition) error
	// Resume restarts fetching.
	Resume(parts []TopicPartition) error
	// Seek moves the next read of a partition to offset, discarding anything
	// already fetched beyond it.
	Seek(tp TopicPartition, offset int64) error
	// Commit synchronously stores the offsets for the consumer group.
	Commit(offsets []TopicPartitionOffset) error
}

// ClientHealth is what a Client knows about its connection to the cluster.
type ClientHealth struct {
	// AllBrokersDown is true while the client cannot reach any broker. Without this
	// signal an unreachable cluster looks exactly like an idle topic.
	AllBrokersDown bool
	// Since is when AllBrokersDown began; zero otherwise.
	Since time.Time
	// LastError is the text of the last error the client library reported. It can
	// name broker addresses but never message data. Empty if none.
	LastError string
}

// HealthReporter is implemented by Clients that can say whether the cluster is
// reachable. The confluent Client does. Consumer.Stats reports it and
// Config.BrokersDownTimeout acts on it.
type HealthReporter interface{ Health() ClientHealth }
