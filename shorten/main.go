package main

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/oklog/ulid"
	"math/rand"
	"time"
)

const TABLE string = "Urls"

type RequestBody struct {
	LongUrl string `json:"url"`
}

type ResponseBody struct {
	ShortUrl string `json:"url"`
}

type Item struct {
	LongUrl  string `json:"LongUrl"`
	ShortUrl string `json:"ShortUrl"`
}

type Request events.APIGatewayProxyRequest
type Response events.APIGatewayProxyResponse

func generateUlid() string {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy).String()
}

func findByLongUrl(url string, svc *dynamodb.DynamoDB) (Item, error) {
	item := Item{}
	flt := expression.Name("LongUrl").Equal(expression.Value(url))
	expr, err := expression.NewBuilder().WithFilter(flt).Build()
	if err != nil {
		return item, err
	}
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		TableName:                 aws.String(TABLE),
	}
	result, err := svc.Scan(params)
	if err != nil {
		return item, err
	}
	if len(result.Items) > 1 {
		return item, errors.New("Found more than one entry")
	}
	if len(result.Items) < 1 {
		return item, nil
	}
	err = dynamodbattribute.UnmarshalMap(result.Items[0], &item)
	if err != nil {
		return item, err
	}
	return item, nil
}

func putItem(item Item, svc *dynamodb.DynamoDB) (*dynamodb.PutItemOutput, error) {
	av, _ := dynamodbattribute.MarshalMap(item)
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(TABLE),
	}
	return svc.PutItem(input)
}

func Handler(request Request) (Response, error) {
	requestBody := RequestBody{
		LongUrl: "",
	}

	err := json.Unmarshal([]byte(request.Body), &requestBody)
	if err != nil {
		return Response{Body: err.Error(), StatusCode: 404}, nil
	}
	sess := session.Must(session.NewSession())
	svc := dynamodb.New(sess)
	item, err := findByLongUrl(requestBody.LongUrl, svc)
	if err != nil {
		return Response{Body: err.Error(), StatusCode: 404}, nil
	}
	if item.ShortUrl == "" {
		item = Item{
			LongUrl:  requestBody.LongUrl,
			ShortUrl: generateUlid(),
		}

		_, err = putItem(item, svc)
		if err != nil {
			return Response{Body: err.Error(), StatusCode: 404}, nil
		}
	}

	return Response{
		Body:            item.ShortUrl,
		StatusCode:      200,
		IsBase64Encoded: false,
		Headers: map[string]string{
			"Content-Type": "application/json",
		}}, nil
}

func main() {
	lambda.Start(Handler)
}
