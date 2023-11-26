package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var endpoint = flag.String("endpoint", "", "s3 endpoint")
var region = flag.String("region", "", "s3 region")
var bucket = flag.String("bucket", "", "bucket name")

// todo - default to other credentials providers
var accessKeyId = flag.String("access-key-id", "", "aws access key")
var secretAccessKey = flag.String("secret-access-key", "", "aws secret key")

var updateExpiresWithin = flag.Int("update-expires-within", 30*24*3600, "only update objects whose lock expires within this many seconds (default 30 days)")
var lockFor = flag.Int("lock-for", 90*24*3600, "how many seconds to renew the object lock for (default 90 days)")

var wg sync.WaitGroup

func main() {
	flag.Parse()

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(*region),
		Endpoint:    aws.String(*endpoint),
		Credentials: credentials.NewStaticCredentials(*accessKeyId, *secretAccessKey, ""),
	})
	if err != nil {
		log.Fatal("Failed to create AWS session:", err)
	}

	svc := s3.New(sess)
	err = svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: bucket,
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range page.Contents {
			wg.Add(1)
			go checkAndRenewObjectLock(svc, *item.Key)
		}

		return !lastPage
	})
	if err != nil {
		log.Fatal("Failed to list objects:", err)
	}

	wg.Wait()
}

func checkAndRenewObjectLock(svc *s3.S3, object string) {
	defer wg.Done()
	retention, err := svc.GetObjectRetention(&s3.GetObjectRetentionInput{
		Bucket: bucket,
		Key:    &object,
	})
	if err != nil {
		log.Fatal("Failed to get retention for", object, err)
	}

	if retention.Retention.RetainUntilDate.Before(time.Now().Add(time.Second * time.Duration(*updateExpiresWithin))) {
		log.Println("Renewing object lock for object", object)
		_, err := svc.PutObjectRetention(&s3.PutObjectRetentionInput{
			Bucket: bucket,
			Key:    &object,
			Retention: &s3.ObjectLockRetention{
				Mode:            aws.String("COMPLIANCE"),
				RetainUntilDate: aws.Time(time.Now().UTC().Add(time.Second * time.Duration(*lockFor))),
			},
		})
		if err != nil {
			log.Fatal("Failed to update retention for", object, err)
		}
	}
}
