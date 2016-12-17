// THIS FILE IS AUTOMATICALLY GENERATED. DO NOT EDIT.

package dynamodbstreams

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/client/metadata"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/private/protocol/jsonrpc"
)

// Amazon DynamoDB Streams provides API actions for accessing streams and processing
// stream records. To learn more about application development with Streams,
// see Capturing Table Activity with DynamoDB Streams (http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)
// in the Amazon DynamoDB Developer Guide.
//
// The following are short descriptions of each low-level DynamoDB Streams action:
//
//    * DescribeStream - Returns detailed information about a particular stream.
//
//    * GetRecords - Retrieves the stream records from within a shard.
//
//    * GetShardIterator - Returns information on how to retrieve the streams
//    record from a shard with a given shard ID.
//
//    * ListStreams - Returns a list of all the streams associated with the
//    current AWS account and endpoint.
//The service client's operations are safe to be used concurrently.
// It is not safe to mutate any of the client's properties though.
type DynamoDBStreams struct {
	*client.Client
}

// Used for custom client initialization logic
var initClient func(*client.Client)

// Used for custom request initialization logic
var initRequest func(*request.Request)

// A ServiceName is the name of the service the client will make API calls to.
const ServiceName = "streams.dynamodb"

// New creates a new instance of the DynamoDBStreams client with a session.
// If additional configuration is needed for the client instance use the optional
// aws.Config parameter to add your extra config.
//
// Example:
//     // Create a DynamoDBStreams client from just a session.
//     svc := dynamodbstreams.New(mySession)
//
//     // Create a DynamoDBStreams client with additional configuration
//     svc := dynamodbstreams.New(mySession, aws.NewConfig().WithRegion("us-west-2"))
func New(p client.ConfigProvider, cfgs ...*aws.Config) *DynamoDBStreams {
	c := p.ClientConfig(ServiceName, cfgs...)
	return newClient(*c.Config, c.Handlers, c.Endpoint, c.SigningRegion, c.SigningName)
}

// newClient creates, initializes and returns a new service client instance.
func newClient(cfg aws.Config, handlers request.Handlers, endpoint, signingRegion, signingName string) *DynamoDBStreams {
	if len(signingName) == 0 {
		signingName = "dynamodb"
	}
	svc := &DynamoDBStreams{
		Client: client.New(
			cfg,
			metadata.ClientInfo{
				ServiceName:   ServiceName,
				SigningName:   signingName,
				SigningRegion: signingRegion,
				Endpoint:      endpoint,
				APIVersion:    "2012-08-10",
				JSONVersion:   "1.0",
				TargetPrefix:  "DynamoDBStreams_20120810",
			},
			handlers,
		),
	}

	// Handlers
	svc.Handlers.Sign.PushBackNamed(v4.SignRequestHandler)
	svc.Handlers.Build.PushBackNamed(jsonrpc.BuildHandler)
	svc.Handlers.Unmarshal.PushBackNamed(jsonrpc.UnmarshalHandler)
	svc.Handlers.UnmarshalMeta.PushBackNamed(jsonrpc.UnmarshalMetaHandler)
	svc.Handlers.UnmarshalError.PushBackNamed(jsonrpc.UnmarshalErrorHandler)

	// Run custom client initialization if present
	if initClient != nil {
		initClient(svc.Client)
	}

	return svc
}

// newRequest creates a new request for a DynamoDBStreams operation and runs any
// custom request initialization.
func (c *DynamoDBStreams) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	// Run custom request initialization if present
	if initRequest != nil {
		initRequest(req)
	}

	return req
}
