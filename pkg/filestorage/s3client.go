package filestorage

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"io"
	"log"
	common "m2cs/pkg"
	"time"
)

type S3Client struct {
	client     *s3.Client
	properties common.ConnectionProperties
}

func (s *S3Client) GetConnectionProperties() common.ConnectionProperties {
	return s.properties
}

func NewS3Client(client *s3.Client, properties common.ConnectionProperties) (*S3Client, error) {
	if client == nil {
		return nil, fmt.Errorf("failed to create S3Client: client is nil")
	}

	_, err := client.ListBuckets(context.TODO(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to AWS S3: %w", err)
	}

	return &S3Client{
		client:     client,
		properties: properties,
	}, nil
}

func (s *S3Client) GetClient() *s3.Client {
	return s.client
}

func (s *S3Client) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName)})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) {
			log.Printf("You already own bucket %s.\n", bucketName)
			err = owned
		} else if errors.As(err, &exists) {
			log.Printf("Bucket %s already exists.\n", bucketName)
			err = exists
		}
	} else {
		err = s3.NewBucketExistsWaiter(s.client).Wait(
			ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for bucket %s to exist.\n", bucketName)
		}
	}

	return err
}

func (s *S3Client) ListBuckets(ctx context.Context) ([]string, error) {
	var err error
	var output *s3.ListBucketsOutput
	var buckets []string
	bucketPaginator := s3.NewListBucketsPaginator(s.client, &s3.ListBucketsInput{})
	for bucketPaginator.HasMorePages() {
		output, err = bucketPaginator.NextPage(ctx)
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "AccessDenied" {
				fmt.Println("You don't have permission to list buckets for this account.")
				err = apiErr
			} else {
				log.Printf("Couldn't list buckets for your account. Here's why: %v\n", err)
			}
			break
		} else {
			for _, bucket := range output.Buckets {
				buckets = append(buckets, fmt.Sprintf("Name: %s, CreatedOn: %s", *bucket.Name, bucket.CreationDate))
			}
		}
	}

	return buckets, err
}

func (s *S3Client) RemoveBucket(ctx context.Context, bucketName string) error {
	_, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName)})
	if err != nil {
		var noBucket *types.NoSuchBucket
		if errors.As(err, &noBucket) {
			log.Printf("Bucket %s does not exist.\n", bucketName)
			err = noBucket
		} else {
			log.Printf("Couldn't delete bucket %v. Here's why: %v\n", bucketName, err)
		}
	} else {
		err = s3.NewBucketNotExistsWaiter(s.client).Wait(
			ctx, &s3.HeadBucketInput{Bucket: aws.String(bucketName)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for bucket %s to be deleted.\n", bucketName)
		} else {
			return nil
		}
	}

	return err
}

func (s *S3Client) GetObject(ctx context.Context, storeBox string, fileName string) (io.ReadCloser, error) {
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(storeBox),
		Key:    aws.String(fileName),
	})
	if err != nil {
		var noKey *types.NoSuchKey
		if errors.As(err, &noKey) {
			log.Printf("Can't get object %s from bucket %s. No such key exists.\n", fileName, storeBox)
			err = noKey
		} else {
			log.Printf("Couldn't get object %v:%v. Here's why: %v\n", storeBox, fileName, err)
		}
		return nil, err
	}

	return result.Body, err
}

func (s *S3Client) PutObject(ctx context.Context, storeBox string, fileName string, reader io.Reader) error {

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(storeBox),
		Key:    aws.String(fileName),
		Body:   reader,
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "EntityTooLarge" {
			log.Printf("Error while uploading object to %s. The object is too large.\n"+
				"To upload objects larger than 5GB, use the S3 console (160GB max)\n"+
				"or the multipart upload API (5TB max).", storeBox)
		} else {
			log.Printf("Couldn't upload file %v to %v. Here's why: %v\n",
				fileName, storeBox, err)
		}
	} else {
		err = s3.NewObjectExistsWaiter(s.client).Wait(
			ctx, &s3.HeadObjectInput{Bucket: aws.String(storeBox), Key: aws.String(fileName)}, time.Minute)
		if err != nil {
			log.Printf("Failed attempt to wait for object %s to exist.\n", fileName)
		}
	}

	return err
}

func (s *S3Client) RemoveObject(ctx context.Context, storeBox string, fileName string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(storeBox),
		Key:    aws.String(fileName),
	}

	_, err := s.client.DeleteObject(ctx, input)
	if err != nil {
		var noKey *types.NoSuchKey
		var apiErr *smithy.GenericAPIError
		if errors.As(err, &noKey) {
			log.Printf("Object %s does not exist in %s.\n", fileName, storeBox)
			err = noKey
		} else if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "AccessDenied":
				log.Printf("Access denied: cannot delete object %s from %s.\n", fileName, storeBox)
				return nil
			}
		} else {
			err = s3.NewObjectNotExistsWaiter(s.client).Wait(
				ctx, &s3.HeadObjectInput{Bucket: aws.String(storeBox), Key: aws.String(fileName)}, time.Minute)
			if err != nil {
				log.Printf("Failed attempt to wait for object %s in bucket %s to be deleted.\n", fileName, storeBox)
			} else {
				return nil
			}
		}
	}

	return err
}
