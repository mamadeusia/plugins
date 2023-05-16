package natsjs

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/nats-io/nats.go"
	"go-micro.dev/v4/events"
	"go-micro.dev/v4/logger"
)

type StreamName string

type StreamConfiguration struct {
	RetentionPolicy nats.RetentionPolicy
	Replicas        int
}

// Options which are used to configure the nats stream.
type Options struct {
	ClusterID           string
	ClientID            string
	Address             string
	NkeyConfig          string
	TLSConfig           *tls.Config
	Logger              logger.Logger
	StreamConfiguration map[StreamName]*StreamConfiguration
}

// Option is a function which configures options.
type Option func(o *Options)

// group config for Read from nats js. (pull based)
type groupReadOptionsContextKey struct{}

func WithReadGroupName(groupName string) events.ReadOption {
	return func(o *events.ReadOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, groupReadOptionsContextKey{}, groupName)
	}
}

type autoAckReadOptionsContextKey struct{}

func WithReadAutoAck(autoAck bool) events.ReadOption {
	return func(o *events.ReadOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, autoAckReadOptionsContextKey{}, autoAck)
	}
}

type ackWaitReadOptionsContextKey struct{}

func WithReadAckWait(ackWait time.Duration) events.ReadOption {
	return func(o *events.ReadOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, ackWaitReadOptionsContextKey{}, ackWait)
	}
}

// retentionPolicy configures the retention policy like limit base - workqueue - interest .
// default configuration is workqueue
func WithStreamRetentionPolicy(streamName string, retentionPolicy nats.RetentionPolicy) Option {
	return func(o *Options) {
		if o.StreamConfiguration == nil {
			o.StreamConfiguration = make(map[StreamName]*StreamConfiguration)
		}
		if o.StreamConfiguration[StreamName(streamName)] == nil {
			o.StreamConfiguration[StreamName(streamName)] = &StreamConfiguration{
				RetentionPolicy: retentionPolicy,
				Replicas:        1,
			}
			return
		} else {
			o.StreamConfiguration[StreamName(streamName)].RetentionPolicy = retentionPolicy
			return
		}

	}
}

// retentionPolicy configures the retention policy like limit base - workqueue - interest .
// default configuration is workqueue
func WithStreamReplicas(streamName string, replicas int) Option {
	return func(o *Options) {
		if o.StreamConfiguration == nil {
			o.StreamConfiguration = make(map[StreamName]*StreamConfiguration)
		}
		if o.StreamConfiguration[StreamName(streamName)] == nil {
			o.StreamConfiguration[StreamName(streamName)] = &StreamConfiguration{
				RetentionPolicy: nats.WorkQueuePolicy,
				Replicas:        replicas,
			}
			return
		} else {
			o.StreamConfiguration[StreamName(streamName)].Replicas = replicas
			return
		}
	}
}

// ClusterID sets the cluster id for the nats connection.
func ClusterID(id string) Option {
	return func(o *Options) {
		o.ClusterID = id
	}
}

// ClientID sets the client id for the nats connection.
func ClientID(id string) Option {
	return func(o *Options) {
		o.ClientID = id
	}
}

// Address of the nats cluster.
func Address(addr string) Option {
	return func(o *Options) {
		o.Address = addr
	}
}

// TLSConfig to use when connecting to the cluster.
func TLSConfig(t *tls.Config) Option {
	return func(o *Options) {
		o.TLSConfig = t
	}
}

// Nkey string to use when connecting to the cluster.
func NkeyConfig(nkey string) Option {
	return func(o *Options) {
		o.NkeyConfig = nkey
	}
}

// Logger sets the underlyin logger
func Logger(log logger.Logger) Option {
	return func(o *Options) {
		o.Logger = log
	}
}
