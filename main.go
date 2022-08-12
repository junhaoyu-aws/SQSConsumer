package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type sqsMessage struct {
	Records []record `json:"Records"`
}

type record struct {
	AWSRegion string `json:"awsRegion"`
	S3        s3     `json:"s3"`
}

type s3 struct {
	Bucket bucket `json:"bucket"`
	Object object `json:"object"`
}

type bucket struct {
	Name string `json:"name"`
}

type object struct {
	Key string `json:"key"`
}

// SQSGetLPMsgAPI defines the interface for the GetQueueUrl and ReceiveMessage functions.
// We use this interface to test the functions using a mocked service.
type SQSGetLPMsgAPI interface {
	GetQueueUrl(ctx context.Context,
		params *sqs.GetQueueUrlInput,
		optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)

	ReceiveMessage(ctx context.Context,
		params *sqs.ReceiveMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

// GetQueueURL gets the URL of an Amazon SQS queue.
// Inputs:
//     c is the context of the method call, which includes the AWS Region.
//     api is the interface that defines the method call.
//     input defines the input arguments to the service call.
// Output:
//     If success, a GetQueueUrlOutput object containing the result of the service call and nil.
//     Otherwise, nil and an error from the call to GetQueueUrl.
func GetQueueURL(c context.Context, api SQSGetLPMsgAPI, input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	return api.GetQueueUrl(c, input)
}

// GetLPMessages gets the messages from an Amazon SQS long polling queue.
// Inputs:
//     c is the context of the method call, which includes the AWS Region.
//     api is the interface that defines the method call.
//     input defines the input arguments to the service call.
// Output:
//     If success, a ReceiveMessageOutput object containing the result of the service call and nil.
//     Otherwise, nil and an error from the call to ReceiveMessage.
func GetLPMessages(c context.Context, api SQSGetLPMsgAPI, input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	return api.ReceiveMessage(c, input)
}

func main() {
	var lastMsgId = ""
	queue := flag.String("q", "", "The name of the queue")
	waitTime := flag.Int("w", 10, "How long the queue waits for messages")
	flag.Parse()
	if *queue == "" {
		fmt.Println("You must supply a queue name (-q QUEUE")
		return
	}

	if *waitTime < 0 {
		*waitTime = 0
	}

	if *waitTime > 20 {
		*waitTime = 20
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("us-west-2"))
	if err != nil {
		panic("configuration error, " + err.Error())
	}

	sqsClient := sqs.NewFromConfig(cfg)

	httpClient := &http.Client{
		Transport: &http.Transport{},
	}

	for {
		time.Sleep(5 * time.Second)

		qInput := &sqs.GetQueueUrlInput{
			QueueName: queue,
		}

		result, err := GetQueueURL(context.TODO(), sqsClient, qInput)
		if err != nil {
			fmt.Println("Got an error getting the queue URL:")
			fmt.Println(err)
			return
		}

		queueURL := result.QueueUrl

		mInput := &sqs.ReceiveMessageInput{
			QueueUrl: queueURL,
			AttributeNames: []types.QueueAttributeName{
				"SentTimestamp",
			},
			MaxNumberOfMessages: 1,
			MessageAttributeNames: []string{
				"All",
			},
			WaitTimeSeconds: int32(*waitTime),
		}

		resp, err := GetLPMessages(context.TODO(), sqsClient, mInput)
		if err != nil {
			fmt.Println("Got an error receiving messages:")
			fmt.Println(err)
			return
		}

		if len(resp.Messages) == 0 {
			continue
		}

		fmt.Println("Latest Message IDs:")

		fmt.Println("    " + *resp.Messages[0].MessageId)

		if lastMsgId != *resp.Messages[0].MessageId { // there is some changes in S3 bucket
			lastMsgId = *resp.Messages[0].MessageId

			// construct a S3 URI using the bucket_name, key, and region info
			// provided by the message
			bodyString := *resp.Messages[0].Body
			msg := &sqsMessage{}
			json.Unmarshal([]byte(bodyString), &msg)
			// log.Println(sqsMessage)
			s3URI := "s3://" + msg.Records[0].S3.Bucket.Name + ".s3." + msg.Records[0].AWSRegion + ".amazonaws.com/" + msg.Records[0].S3.Object.Key

			// send a HTTP signal to the running Collector, then do hot-reload
			req, err := http.NewRequest("GET", "http://localhost:5555/configHotReload", strings.NewReader(s3URI))
			if err != nil {
				panic(err)
			}
			req.Close = true

			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Connection", "Keep-Alive")
			resp, err := httpClient.Do(req)
			if err != nil {
				panic(err)
			}

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}

			fmt.Printf("%s\n", string(body))
		}
	}
}
