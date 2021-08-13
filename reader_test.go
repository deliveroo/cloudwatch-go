package cloudwatch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/suite"
)

type readerTestSuite struct {
	suite.Suite

	api                   *mockAPI
	ctx                   context.Context
	groupName, streamName string
	sut                   io.ReadCloser
}

func (r *readerTestSuite) SetupTest() {
	r.api = new(mockAPI)
	r.ctx = context.Background()
	r.groupName = "groupName"
	r.streamName = "streamName"

	r.sut = &readerImpl{
		client:     r.api,
		ctx:        r.ctx,
		groupName:  aws.String(r.groupName),
		streamName: aws.String(r.streamName),
		throttle:   time.NewTicker(time.Nanosecond),
	}
}

func (r *readerTestSuite) TearDownTest() {
	r.Require().NoError(r.sut.Close())
}

func (r *readerTestSuite) TestSimpleRead() {
	r.api.On(
		"GetLogEventsWithContext",
		r.ctx,
		&cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(r.groupName),
			LogStreamName: aws.String(r.streamName),
			StartFromHead: aws.Bool(true),
		},
		[]request.Option(nil),
	).Once().Return(&cloudwatchlogs.GetLogEventsOutput{
		Events: []*cloudwatchlogs.OutputLogEvent{
			{Message: aws.String("Hello"), Timestamp: aws.Int64(1000)},
		},
	}, nil)

	r.NoError(r.sut.(*readerImpl).read())

	buffer := make([]byte, 1000)
	n, err := r.sut.Read(buffer)

	r.NoError(err)
	r.Equal(5, n)
}

func (r *readerTestSuite) TestBuffering() {
	r.api.On(
		"GetLogEventsWithContext",
		r.ctx,
		&cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(r.groupName),
			LogStreamName: aws.String(r.streamName),
			StartFromHead: aws.Bool(true),
		},
		[]request.Option(nil),
	).Once().Return(&cloudwatchlogs.GetLogEventsOutput{
		Events: []*cloudwatchlogs.OutputLogEvent{
			{Message: aws.String("Hello"), Timestamp: aws.Int64(1000)},
		},
	}, nil)

	r.NoError(r.sut.(*readerImpl).read())

	buffer := make([]byte, 3)

	n, err := r.sut.Read(buffer)
	r.NoError(err)
	r.Equal(3, n)
	r.Equal("Hel", string(buffer[:n]))

	n, err = r.sut.Read(buffer)
	r.NoError(err)
	r.Equal(2, n)
	r.Equal("lo", string(buffer[:n]))
}

func (r *readerTestSuite) TestEndOfFile() {
	r.api.On(
		"GetLogEventsWithContext",
		r.ctx,
		&cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(r.groupName),
			LogStreamName: aws.String(r.streamName),
			StartFromHead: aws.Bool(true),
		},
		[]request.Option(nil),
	).Once().Return(&cloudwatchlogs.GetLogEventsOutput{
		Events: []*cloudwatchlogs.OutputLogEvent{
			{Message: aws.String("Hello"), Timestamp: aws.Int64(1000)},
		},
		NextForwardToken: aws.String("next"),
	}, nil)

	r.api.On(
		"GetLogEventsWithContext",
		r.ctx,
		&cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(r.groupName),
			LogStreamName: aws.String(r.streamName),
			StartFromHead: aws.Bool(true),
			NextToken:     aws.String("next"),
		},
		[]request.Option(nil),
	).Once().Return(&cloudwatchlogs.GetLogEventsOutput{
		Events: []*cloudwatchlogs.OutputLogEvent{
			{Message: aws.String("World"), Timestamp: aws.Int64(1000)},
		},
	}, nil)

	r.api.On(
		"GetLogEventsWithContext",
		r.ctx,
		&cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(r.groupName),
			LogStreamName: aws.String(r.streamName),
			StartFromHead: aws.Bool(true),
			NextToken:     aws.String("next"),
		},
		[]request.Option(nil),
	).Once().Return(&cloudwatchlogs.GetLogEventsOutput{
		Events: []*cloudwatchlogs.OutputLogEvent{},
	}, nil)

	r.NoError(r.sut.(*readerImpl).read())
	buffer := make([]byte, 5)
	n, err := r.sut.Read(buffer)
	r.NoError(err)
	r.Equal(5, n)
	r.EqualValues("Hello", buffer[:n])

	r.NoError(r.sut.(*readerImpl).read())
	n, err = r.sut.Read(buffer)
	r.NoError(err)
	r.Equal(5, n)
	r.EqualValues("World", buffer[:n])

	r.NoError(r.sut.(*readerImpl).read())
	n, err = r.sut.Read(buffer)
	r.NoError(err)
	r.Equal(0, n)
}

func (r *readerTestSuite) TestReadError() {
	r.sut = NewGroup(r.api, r.groupName).Open(r.ctx, r.streamName)

	const errorMessage = "boom!"
	r.api.On(
		"GetLogEventsWithContext",
		r.ctx,
		&cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(r.groupName),
			LogStreamName: aws.String(r.streamName),
			StartFromHead: aws.Bool(true),
		},
		[]request.Option(nil),
	).Once().Return(&cloudwatchlogs.GetLogEventsOutput{
		Events: []*cloudwatchlogs.OutputLogEvent{
			{Message: aws.String("Hello"), Timestamp: aws.Int64(1000)},
		},
	}, errors.New(errorMessage))

	buffer := new(bytes.Buffer)
	_, err := io.Copy(buffer, r.sut)
	r.EqualError(err, errorMessage)
}

func TestReader(t *testing.T) {
	suite.Run(t, new(readerTestSuite))
}
