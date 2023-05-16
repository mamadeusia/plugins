package natsjs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/pkg/errors"

	"go-micro.dev/v4/events"
	"go-micro.dev/v4/logger"
)

const (
	defaultClusterID = "micro"
)

// NewStream returns an initialized nats stream or an error if the connection to the nats
// server could not be established.
func NewStream(opts ...Option) (events.Stream, error) {
	// parse the options
	options := Options{
		ClientID:  uuid.New().String(),
		ClusterID: defaultClusterID,
		Logger:    logger.DefaultLogger,
	}
	for _, o := range opts {
		o(&options)
	}

	s := &stream{opts: options}
	natsJetStreamCtx, err := connectToNatsJetStream(options)
	if err != nil {
		return nil, fmt.Errorf("error connecting to nats cluster %v: %v", options.ClusterID, err)
	}
	s.natsJetStreamCtx = natsJetStreamCtx
	return s, nil
}

type store struct {
	opts             Options
	natsJetStreamCtx nats.JetStreamContext
	topicSubs        map[string]*nats.Subscription
}

type stream struct {
	opts             Options
	natsJetStreamCtx nats.JetStreamContext
}

func connectToNatsJetStream(options Options) (nats.JetStreamContext, error) {
	nopts := nats.GetDefaultOptions()
	if options.TLSConfig != nil {
		nopts.Secure = true
		nopts.TLSConfig = options.TLSConfig
	}
	if options.NkeyConfig != "" {
		kp, err := nkeys.FromSeed([]byte(options.NkeyConfig))
		if err != nil {
			return nil, err
		}
		pubKey, err := kp.PublicKey()
		if err != nil {
			return nil, err
		}
		nopts.Nkey = pubKey

		nopts.SignatureCB = func(nonce []byte) ([]byte, error) {
			kp, err := nkeys.FromSeed([]byte(options.NkeyConfig))
			if err != nil {
				return nil, err
			}
			sig, err := kp.Sign(nonce)
			if err != nil {
				return nil, err
			}
			return sig, nil
		}

	}

	if len(options.Address) > 0 {
		nopts.Servers = strings.Split(options.Address, ",")
	}

	conn, err := nopts.Connect()
	if err != nil {
		return nil, fmt.Errorf("error connecting to nats at %v with tls enabled (%v): %v", options.Address, nopts.TLSConfig != nil, err)
	}

	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("error while obtaining JetStream context: %v", err)
	}
	for streamName, conf := range options.StreamConfiguration {
		_, err := js.StreamInfo(string(streamName))
		if err != nil {
			cfg := &nats.StreamConfig{
				Name: string(streamName),
				// Subjects:  []string{},
				Retention: conf.RetentionPolicy,
				Replicas:  conf.Replicas,
			}

			_, err := js.AddStream(cfg)
			if err != nil {
				return nil, errors.Wrap(err, "Stream did not exist and adding a stream failed")
			}

		}
	}

	return js, nil
}

// Publish a message to a topic.
func (s *stream) Publish(topic string, msg interface{}, opts ...events.PublishOption) error {
	// validate the topic
	if len(topic) == 0 {
		return events.ErrMissingTopic
	}

	// validate the topic
	splits := strings.Split(topic, ".")
	var streamSubject string
	if len(splits) >= 2 {
		if !IsUpper(splits[0]) {
			return fmt.Errorf("name must be uppercase")
		}

		streamName := splits[0]
		streamSubject = strings.Replace(topic, streamName+".", "", 1)

		// ensure that a stream exists for that topic
		streamInfo, err := s.natsJetStreamCtx.StreamInfo(streamName)
		if err != nil {
			cfg := &nats.StreamConfig{
				Name:      streamName,
				Subjects:  []string{streamSubject},
				Retention: nats.WorkQueuePolicy, // default retention policy
			}

			_, err := s.natsJetStreamCtx.AddStream(cfg)
			if err != nil {
				return errors.Wrap(err, "Stream did not exist and adding a stream failed")
			}

		} else if !stringContains(streamInfo.Config.Subjects, streamSubject) {
			olderSubjects := streamInfo.Config.Subjects
			newSubjects := append(olderSubjects, streamSubject)
			streamInfo.Config.Subjects = newSubjects

			_, err := s.natsJetStreamCtx.UpdateStream(&streamInfo.Config)
			if err != nil {
				return errors.Wrap(err, "Stream add subject failed")
			}

		}

	}

	// parse the options
	options := events.PublishOptions{
		Timestamp: time.Now(),
	}
	for _, o := range opts {
		o(&options)
	}

	// encode the message if it's not already encoded
	var payload []byte
	if p, ok := msg.([]byte); ok {
		payload = p
	} else {
		p, err := json.Marshal(msg)
		if err != nil {
			return events.ErrEncodingMessage
		}
		payload = p
	}

	// construct the event
	event := &events.Event{
		ID:        uuid.New().String(),
		Topic:     topic,
		Timestamp: options.Timestamp,
		Metadata:  options.Metadata,
		Payload:   payload,
	}

	// serialize the event to bytes
	bytes, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "Error encoding event")
	}
	if len(splits) >= 2 {
		// publish the event to the topic's channel
		if _, err := s.natsJetStreamCtx.PublishAsync(streamSubject, bytes); err != nil {
			return errors.Wrap(err, "Error publishing message to topic")
		}
	} else {
		// publish the event to the topic's channel
		if _, err := s.natsJetStreamCtx.PublishAsync(event.Topic, bytes); err != nil {
			return errors.Wrap(err, "Error publishing message to topic")
		}

	}

	return nil
}

// Consume from a topic.
func (s *stream) Consume(topic string, opts ...events.ConsumeOption) (<-chan events.Event, error) {
	// validate the topic
	if len(topic) == 0 {
		return nil, events.ErrMissingTopic
	}

	log := s.opts.Logger

	// parse the options
	options := events.ConsumeOptions{
		Group: uuid.New().String(),
	}
	for _, o := range opts {
		o(&options)
	}

	// setup the subscriber
	c := make(chan events.Event)
	handleMsg := func(m *nats.Msg) {
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		// decode the message
		var evt events.Event
		if err := json.Unmarshal(m.Data, &evt); err != nil {
			log.Logf(logger.ErrorLevel, "Error decoding message: %v", err)
			// not acknowledging the message is the way to indicate an error occurred
			return
		}

		if !options.AutoAck {
			// set up the ack funcs
			evt.SetAckFunc(func() error {
				return m.Ack()
			})
			evt.SetNackFunc(func() error {
				return m.Nak()
			})
		}

		// push onto the channel and wait for the consumer to take the event off before we acknowledge it.
		c <- evt

		if !options.AutoAck {
			return
		}
		if err := m.Ack(nats.Context(ctx)); err != nil {
			log.Logf(logger.ErrorLevel, "Error acknowledging message: %v", err)
		}

	}

	// validate the topic
	splits := strings.Split(topic, ".")
	var streamSubject string
	if len(splits) >= 2 {
		if !IsUpper(splits[0]) {
			return nil, fmt.Errorf("name must be uppercase")
		}

		streamName := splits[0]
		streamSubject = strings.Replace(topic, streamName+".", "", 1)

		// ensure that a stream exists for that topic
		streamInfo, err := s.natsJetStreamCtx.StreamInfo(streamName)
		if err != nil {
			cfg := &nats.StreamConfig{
				Name:      streamName,
				Subjects:  []string{streamSubject},
				Retention: nats.WorkQueuePolicy, // default retention
			}

			_, err := s.natsJetStreamCtx.AddStream(cfg)
			if err != nil {
				return nil, errors.Wrap(err, "Stream did not exist and adding a stream failed")
			}

		} else if !stringContains(streamInfo.Config.Subjects, streamSubject) {
			olderSubjects := streamInfo.Config.Subjects
			newSubjects := append(olderSubjects, streamSubject)
			streamInfo.Config.Subjects = newSubjects

			_, err := s.natsJetStreamCtx.UpdateStream(&streamInfo.Config)
			if err != nil {
				return nil, errors.Wrap(err, "Stream add subject failed")
			}

		}
	} else {
		// ensure that a stream exists for that topic
		_, err := s.natsJetStreamCtx.StreamInfo(topic)
		if err != nil {
			cfg := &nats.StreamConfig{
				Name:      topic,
				Retention: nats.WorkQueuePolicy, // default retention
			}

			_, err = s.natsJetStreamCtx.AddStream(cfg)
			if err != nil {
				return nil, errors.Wrap(err, "Stream did not exist and adding a stream failed")
			}
		}

	}

	// setup the options
	subOpts := []nats.SubOpt{
		nats.Durable(options.Group),
	}

	if options.CustomRetries {
		subOpts = append(subOpts, nats.MaxDeliver(options.GetRetryLimit()))
	}

	if options.AutoAck {
		subOpts = append(subOpts, nats.AckNone())
	} else {
		subOpts = append(subOpts, nats.AckExplicit())
	}

	if !options.Offset.IsZero() {
		subOpts = append(subOpts, nats.StartTime(options.Offset))
	} else {
		subOpts = append(subOpts, nats.DeliverNew())
	}

	if options.AckWait > 0 {
		subOpts = append(subOpts, nats.AckWait(options.AckWait))
	}
	if len(splits) >= 2 {
		uniqueGroupName := options.Group + strings.Replace(topic, ".", "", -1)

		_, err := s.natsJetStreamCtx.QueueSubscribe(streamSubject, uniqueGroupName, handleMsg, subOpts...)
		if err != nil {
			return nil, errors.Wrap(err, "Error subscribing to topic")
		}
	} else {
		_, err := s.natsJetStreamCtx.QueueSubscribe(topic, options.Group, handleMsg, subOpts...)
		if err != nil {
			return nil, errors.Wrap(err, "Error subscribing to topic")
		}
	}
	// connect the subscriber

	return c, nil
}

func NewStore(opts ...Option) (events.Store, error) {
	// parse the options
	options := Options{
		ClientID:  uuid.New().String(),
		ClusterID: defaultClusterID,
		Logger:    logger.DefaultLogger,
	}
	for _, o := range opts {
		o(&options)
	}

	s := &store{opts: options}
	natsJetStreamCtx, err := connectToNatsJetStream(options)
	if err != nil {
		return nil, fmt.Errorf("error connecting to nats cluster %v: %v", options.ClusterID, err)
	}
	s.natsJetStreamCtx = natsJetStreamCtx

	s.topicSubs = make(map[string]*nats.Subscription)

	return s, nil
}

func IsUpper(s string) bool {
	for _, r := range s {
		if !unicode.IsUpper(r) && unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func stringContains(s []string, e string) bool {
	for _, r := range s {
		if r == e {
			return true
		}
	}
	return false
}

// pull base
func (s *store) Read(topic string, opts ...events.ReadOption) ([]*events.Event, error) {
	// validate the topic
	if len(topic) == 0 {
		return nil, events.ErrMissingTopic
	}
	options := &events.ReadOptions{
		Limit: 1,
	}
	for _, o := range opts {
		o(options)
	}

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	splits := strings.Split(topic, ".")
	var streamSubject string

	if len(splits) >= 2 {
		if !IsUpper(splits[0]) {
			return nil, fmt.Errorf("name must be uppercase")
		}

		streamName := splits[0]
		streamSubject := strings.Replace(topic, streamName+".", "", 1)

		streamInfo, err := s.natsJetStreamCtx.StreamInfo(streamName)
		if err != nil {
			cfg := &nats.StreamConfig{
				Name:      streamName,
				Subjects:  []string{streamSubject},
				Retention: nats.WorkQueuePolicy, // default retention
			}

			_, err := s.natsJetStreamCtx.AddStream(cfg)
			if err != nil {
				return nil, errors.Wrap(err, "Stream did not exist and adding a stream failed")
			}

		} else if !stringContains(streamInfo.Config.Subjects, streamSubject) {
			olderSubjects := streamInfo.Config.Subjects
			newSubjects := append(olderSubjects, streamSubject)
			streamInfo.Config.Subjects = newSubjects

			_, err := s.natsJetStreamCtx.UpdateStream(&streamInfo.Config)
			if err != nil {
				return nil, errors.Wrap(err, "Stream add subject failed")
			}

		}
	} else {
		// ensure that a stream exists for that topic
		_, err := s.natsJetStreamCtx.StreamInfo(topic)
		if err != nil {
			cfg := &nats.StreamConfig{
				Name:      topic,
				Retention: nats.WorkQueuePolicy, // default retention
			}

			_, err = s.natsJetStreamCtx.AddStream(cfg)
			if err != nil {
				return nil, errors.Wrap(err, "Stream did not exist and adding a stream failed")
			}
		}

	}

	log := s.opts.Logger

	subOpts := []nats.SubOpt{}

	autoAck, ok := options.Context.Value(autoAckReadOptionsContextKey{}).(bool)
	if ok {
		if autoAck {
			subOpts = append(subOpts, nats.AckNone())
		} else {
			subOpts = append(subOpts, nats.AckExplicit())

		}
	} else {
		subOpts = append(subOpts, nats.AckNone())
	}

	ackWait, ok := options.Context.Value(ackWaitReadOptionsContextKey{}).(time.Duration)
	if ok {
		if ackWait > 0 {
			subOpts = append(subOpts, nats.AckWait(ackWait))

		}

	}

	if _, ok := s.topicSubs[topic]; !ok {
		if len(splits) >= 2 {

			groupName, ok := options.Context.Value(groupReadOptionsContextKey{}).(string)
			if !ok {
				// TODO :: test it to work or not.
				sub, err := s.natsJetStreamCtx.PullSubscribe(streamSubject, "", subOpts...)
				if err != nil {
					log.Logf(logger.ErrorLevel, "Error pull subscribe: %v", err)
					return nil, err
				}
				s.topicSubs[topic] = sub
			} else {
				uniqueGroupName := groupName + strings.Replace(topic, ".", "", -1)
				sub, err := s.natsJetStreamCtx.PullSubscribe(streamSubject, uniqueGroupName, subOpts...)
				if err != nil {
					log.Logf(logger.ErrorLevel, "Error pull subscribe: %v", err)
					return nil, err
				}
				s.topicSubs[topic] = sub
			}
		} else {
			// it this scenario I think we should pull all the stream.
			// maybe we should prohibit this.
			subOpts = append(subOpts, nats.BindStream(topic))
			sub, err := s.natsJetStreamCtx.PullSubscribe("", "", subOpts...)
			if err != nil {
				log.Logf(logger.ErrorLevel, "Error pull subscribe: %v", err)
				return nil, err
			}
			s.topicSubs[topic] = sub
		}

	}

	msgs, err := s.topicSubs[topic].Fetch(int(options.Limit), nats.MaxWait(3*time.Second))

	if err != nil {
		log.Logf(logger.ErrorLevel, "Error fetching messages: %v", err)
		return nil, err
	}
	var evts []*events.Event
	for _, m := range msgs {
		// decode the message
		var evt events.Event
		if err := json.Unmarshal(m.Data, &evt); err != nil {
			logger.Logf(logger.ErrorLevel, "Error decoding message: %v", err)
			// not acknowledging the message is the way to indicate an error occurred
			return nil, err
		}
		if !autoAck {
			// set up the ack funcs
			evt.SetAckFunc(func() error {
				return m.Ack()
			})
			evt.SetNackFunc(func() error {
				return m.Nak()
			})
		}
		evts = append(evts, &evt)

		if autoAck {
			m.Ack(nats.Context(ctx))
		}
	}

	return evts, nil
}

// need to think about it that do we need it or not
// its same as publish
func (s *store) Write(event *events.Event, opts ...events.WriteOption) error {
	return nil
}
