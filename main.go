package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var endpoint = flag.String("endpoint", "", "s3 endpoint")
var region = flag.String("region", "us-east-1", "s3 region")

var accessKeyId = flag.String("access-key-id", "", "aws access key")
var secretAccessKey = flag.String("secret-access-key", "", "aws secret key")

var bucket = flag.String("bucket", "", "bucket name")
var prefix = flag.String("prefix", "", "only operate on objects starting with this prefix")

var updateExpiresWithin = flag.Int("update-expires-within", 0, "only update objects whose lock expires within this many seconds (default 0)")
var lockFor = flag.Int("lock-for", 90*24*3600, "how many seconds to renew the object lock for (default 90 days)")

var threadCount = flag.Int("threads", 1024, "how many objects to operate on at a time")

func main() {
	flag.Parse()

	options := []func(*config.LoadOptions) error{}

	if *region != "" {
		options = append(options, config.WithRegion(*region))
	}

	if *endpoint != "" {
		options = append(options, config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: *endpoint,
				}, nil
			},
		)))
	}

	if *accessKeyId != "" && *secretAccessKey != "" {
		options = append(options, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*accessKeyId, *secretAccessKey, "")))
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), options...)
	if err != nil {
		log.Fatalln("Failed to create AWS config", err)
	}

	svc := s3.NewFromConfig(cfg)

	paginator := s3.NewListObjectsV2Paginator(svc, &s3.ListObjectsV2Input{
		Bucket: bucket,
		Prefix: prefix,
	})

	objectQueue := make(chan string)

	var wg sync.WaitGroup
	for i := 0; i < *threadCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			queueWorker(svc, objectQueue)
		}()
	}

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Fatalln("Failed to list objects", err)
		}

		for _, item := range page.Contents {
			objectQueue <- *item.Key
		}
	}

	close(objectQueue)
	wg.Wait()
}

func queueWorker(svc *s3.Client, inQueue chan string) {
	for {
		object, more := <-inQueue
		if !more {
			return
		}
		checkAndRenewObjectLock(svc, object)
	}
}

func checkAndRenewObjectLock(svc *s3.Client, object string) {
	updateHold := false
	if *updateExpiresWithin == 0 {
		updateHold = true
	} else {
		retention, _ := svc.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
			Bucket: bucket,
			Key:    &object,
		})
		if retention == nil {
			updateHold = true
		} else if retention.Retention.RetainUntilDate.Before(time.Now().Add(time.Second * time.Duration(*updateExpiresWithin))) {
			updateHold = true
		}
	}

	if updateHold {
		log.Println("Renewing object lock for object", object)
		_, err := svc.PutObjectRetention(context.TODO(), &s3.PutObjectRetentionInput{
			Bucket: bucket,
			Key:    &object,
			Retention: &types.ObjectLockRetention{
				Mode:            "COMPLIANCE",
				RetainUntilDate: aws.Time(time.Now().Add(time.Second * time.Duration(*lockFor))),
			},
		})
		if err != nil {
			log.Fatalln("Failed to update retention for", object, err)
		}
	}
}
