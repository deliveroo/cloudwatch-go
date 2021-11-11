package cloudwatch

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

// RejectedLogEventsInfoError wraps `cloudwatchlogs.RejectedLogEventsInfo` from
// the AWS SDK, and makes it an implementation of Go's error interface.
type RejectedLogEventsInfoError struct {
	Info *cloudwatchlogs.RejectedLogEventsInfo
}

func (e *RejectedLogEventsInfoError) Error() string {
	return "log messages were rejected"
}

// CreateOption allows setting various options on the resulting writer.
type CreateOption func(*writerImpl)

// Group is an abstraction over AWS CloudWatch Logs Group, allowing one to treat
// it like a remote io.ReadWriter.
type Group interface {
	cloudwatchlogsiface.CloudWatchLogsAPI

	// Create creates a log stream in the managed group and returns an
	// implementation of io.Writer to write to it.
	Create(ctx context.Context, streamName string, opts ...CreateOption) (io.WriteCloser, error)

	// Name of the CloudWatch Logs group owned by this proxy.
	Name() string

	// Open returns an io.Readcloser to read from the log stream.
	Open(ctx context.Context, streamName string) io.ReadCloser
}
