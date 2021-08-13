package cloudwatch

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	iface "github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
)

type readerImpl struct {
	groupName, streamName, nextToken *string

	client iface.CloudWatchLogsAPI
	ctx    context.Context

	throttle *time.Ticker
	buffer   lockingBuffer

	// If an error occurs when getting events from the stream, this will be
	// populated and subsequent calls to Read will return the error.
	err error
}

func (r *readerImpl) Read(b []byte) (int, error) {
	// Return the AWS error if there is one.
	if r.err != nil {
		return 0, r.err
	}
	// If there is not data right now, return. Reading from the buffer would
	// result in io.EOF being returned, which is not what we want.
	if r.buffer.Len() == 0 {
		return 0, nil
	}
	return r.buffer.Read(b)
}

func (r *readerImpl) Close() error {
	r.throttle.Stop()
	return nil
}

func (r *readerImpl) start() {
	for {
		<-r.throttle.C
		if r.err = r.read(); r.err != nil {
			return
		}
	}
}

func (r *readerImpl) read() error {
	input := &cloudwatchlogs.GetLogEventsInput{
		LogGroupName:  r.groupName,
		LogStreamName: r.streamName,
		StartFromHead: aws.Bool(true),
		NextToken:     r.nextToken,
	}

	resp, err := r.client.GetLogEventsWithContext(r.ctx, input)

	if err != nil {
		return err
	}

	// We want to re-use the existing token in the event that
	// NextForwardToken is nil, which means there's no new messages to
	// consume.
	if resp.NextForwardToken != nil {
		r.nextToken = resp.NextForwardToken
	}

	// If there are no messages, return so that the consumer can read again.
	if len(resp.Events) == 0 {
		return nil
	}

	for _, event := range resp.Events {
		r.buffer.WriteString(*event.Message)
	}

	return nil
}

// lockingBuffer is a bytes.Buffer that locks Reads and Writes.
type lockingBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (r *lockingBuffer) Read(b []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	return r.Buffer.Read(b)
}

func (r *lockingBuffer) Write(b []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	return r.Buffer.Write(b)
}
