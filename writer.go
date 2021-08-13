package cloudwatch

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	iface "github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

type writerImpl struct {
	client iface.CloudWatchLogsAPI

	groupName, streamName, sequenceToken *string

	ctx context.Context

	closed bool
	err    error

	events  *eventsBuffer
	nowFunc func() time.Time
	onEvent func(*cloudwatchlogs.InputLogEvent)

	throttle *time.Ticker

	sync.Mutex // This protects calls to flush.
}

// WithInputCallback allows setting a function introspecting each input log
// event before it's sent to AWS CloudWatch Logs.
func WithInputCallback(callback func(*cloudwatchlogs.InputLogEvent)) CreateOption {
	return func(w *writerImpl) {
		w.onEvent = callback
	}
}

// FromToken allows writing from an arbitrary sequence token.
func FromToken(sequenceToken string) CreateOption {
	return func(w *writerImpl) {
		w.sequenceToken = aws.String(sequenceToken)
	}
}

func freezeTime(now time.Time) CreateOption {
	return func(w *writerImpl) {
		w.nowFunc = func() time.Time {
			return now
		}
	}
}

// Write takes the buffer, and creates a Cloudwatch Log event for each
// individual line. If Flush returns an error, subsequent calls to Write will
// fail.
func (w *writerImpl) Write(b []byte) (int, error) {
	if w.closed {
		return 0, io.ErrClosedPipe
	}

	if w.err != nil {
		return 0, w.err
	}

	return w.buffer(b)
}

// Start continuously flushing the buffered events.
func (w *writerImpl) start() error {
	for {
		// Exit if the stream is closed.
		if w.closed {
			return nil
		}

		if err := w.flushTrottled(); err != nil {
			return err
		}
	}
}

// Close closes the writer. Any subsequent calls to Write will return
// io.ErrClosedPipe.
func (w *writerImpl) Close() error {
	defer w.throttle.Stop()

	w.closed = true
	for w.events.hasMore() {
		if w.flushTrottled() != nil {
			break
		}
	}
	return w.err
}

func (w *writerImpl) flushTrottled() error {
	<-w.throttle.C
	return w.flushBatch()
}

func (w *writerImpl) flushBatch() error {
	w.Lock()
	defer w.Unlock()

	events := w.events.drain()

	// No events to flush.
	if len(events) == 0 {
		return nil
	}

	w.err = w.flush(events)
	return w.err
}

// flush flushes a slice of log events. This method should be called
// sequentially to ensure that the sequence token is updated properly.
func (w *writerImpl) flush(events []*cloudwatchlogs.InputLogEvent) (err error) {
	var resp *cloudwatchlogs.PutLogEventsOutput

	for {
		resp, err = w.client.PutLogEventsWithContext(w.ctx, &cloudwatchlogs.PutLogEventsInput{
			LogEvents:     events,
			LogGroupName:  w.groupName,
			LogStreamName: w.streamName,
			SequenceToken: w.sequenceToken,
		})

		if err == nil {
			break
		}

		sequenceError, ok := err.(*cloudwatchlogs.InvalidSequenceTokenException)
		if !ok {
			return err
		}

		w.sequenceToken = sequenceError.ExpectedSequenceToken
	}

	if resp.RejectedLogEventsInfo != nil {
		w.err = &RejectedLogEventsInfoError{Info: resp.RejectedLogEventsInfo}
		return w.err
	}

	w.sequenceToken = resp.NextSequenceToken

	return nil
}

// buffer splits up b into individual log events and inserts them into the
// buffer.
func (w *writerImpl) buffer(b []byte) (int, error) {
	r := bufio.NewReader(bytes.NewReader(b))

	var (
		n   int
		eof bool
	)

	for !eof {
		b, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				eof = true
			} else {
				break
			}
		}

		if len(b) == 0 {
			continue
		}

		event := &cloudwatchlogs.InputLogEvent{
			Message:   aws.String(string(b)),
			Timestamp: aws.Int64(w.now().UnixNano() / 1000000),
		}

		if w.onEvent != nil {
			w.onEvent(event)
		}

		w.events.add(event)

		n += len(b)
	}

	return n, nil
}

func (w *writerImpl) now() time.Time {
	if w.nowFunc == nil {
		return time.Now()
	}
	return w.nowFunc()
}
