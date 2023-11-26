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
var bucket = flag.String("bucket", "", "bucket name")

// todo - default to other credentials providers
var accessKeyId = flag.String("access-key-id", "", "aws access key")
var secretAccessKey = flag.String("secret-access-key", "", "aws secret key")

var updateExpiresWithin = flag.Int("update-expires-within", 30*24*3600, "only update objects whose lock expires within this many seconds (default 30 days)")
var lockFor = flag.Int("lock-for", 90*24*3600, "how many seconds to renew the object lock for (default 90 days)")

var wg sync.WaitGroup

func main() {
	flag.Parse()

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		// just a placeholder because it needs to be defined
		// should be using teh one from the endpoint regardless
		config.WithRegion("us-east-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: *endpoint,
				}, nil
			},
		)),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*accessKeyId, *secretAccessKey, "")),
	)
	if err != nil {
		log.Fatal("Failed to create AWS config", err)
	}

	svc := s3.NewFromConfig(cfg)

	paginator := s3.NewListObjectsV2Paginator(svc, &s3.ListObjectsV2Input{
		Bucket: bucket,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Fatal("Failed to list objects", err)
		}

		for _, item := range page.Contents {
			wg.Add(1)
			go checkAndRenewObjectLock(svc, *item.Key)
		}
	}

	wg.Wait()
}

func checkAndRenewObjectLock(svc *s3.Client, object string) {
	defer wg.Done()

	retention, err := svc.GetObjectRetention(context.TODO(), &s3.GetObjectRetentionInput{
		Bucket: bucket,
		Key:    &object,
	})
	if err != nil {
		log.Fatal("Failed to get retention for", object, err)
	}

	if retention.Retention.RetainUntilDate.Before(time.Now().Add(time.Second * time.Duration(*updateExpiresWithin))) {
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
			log.Fatal("Failed to update retention for", object, err)
		}
	}
}
