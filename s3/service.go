package s3

import (
	"context"
	"fmt"
	"log"
	"net/url"
	s3Domain "poc/s3-client/domain/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/private/protocol/rest"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func NewS3Client(id, secret, token string, region *string) *S3 {
	session, err := session.NewSession(&aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentials(id, secret, token),
	})
	if err != nil {
		panic(err)
	}
	svc := s3.New(session)

	s3Client := &S3{
		Session: session,
		Client:  svc,
	}
	return s3Client
}

func (svc *S3) GetListObjects(input s3Domain.GetListObjectsInput) ([]*s3.Object, error) {
	requetObject := &s3.ListObjectsV2Input{
		Bucket: aws.String(input.Bucket),
		Prefix: aws.String(input.Prefix),
	}

	resp, err := svc.Client.ListObjectsV2(requetObject)
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", input.Bucket, err)
		return nil, err
	}

	return resp.Contents, nil
}

func (svc *S3) GetListObjectsWithContext(ctx context.Context, input s3Domain.GetListObjectsInput) ([]*s3.Object, error) {

	requetObject := &s3.ListObjectsV2Input{
		Bucket: aws.String(input.Bucket),
		Prefix: aws.String(input.Prefix),
	}

	resp, err := svc.Client.ListObjectsV2WithContext(ctx, requetObject)
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", input.Bucket, err)
		return nil, err
	}

	return resp.Contents, nil
}

func (svc *S3) DownloadFile(input s3Domain.DownloadFileInput) (*aws.WriteAtBuffer, error) {
	downloader := s3manager.NewDownloader(svc.Session)

	requestInput := s3.GetObjectInput{
		Bucket: aws.String(input.Bucket),
		Key:    aws.String(input.Item),
	}

	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(buf, &requestInput)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (svc *S3) DownloadFileWithContext(ctx context.Context, input s3Domain.DownloadFileInput) (*aws.WriteAtBuffer, error) {
	downloader := s3manager.NewDownloader(svc.Session)

	requestInput := s3.GetObjectInput{
		Bucket: aws.String(input.Bucket),
		Key:    aws.String(input.Item),
	}

	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := downloader.DownloadWithContext(ctx, buf, &requestInput)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (svc *S3) UploadFile(input s3Domain.UploadFileInput) (*s3manager.UploadOutput, error) {
	uploader := s3manager.NewUploader(svc.Session)

	requestInput := &s3manager.UploadInput{
		Bucket: aws.String(input.Bucket),
		Key:    aws.String(input.Item),
		Body:   input.File,
	}

	output, err := uploader.Upload(requestInput)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (svc *S3) UploadFileWithContext(ctx context.Context, input s3Domain.UploadFileInput) (*s3manager.UploadOutput, error) {
	uploader := s3manager.NewUploader(svc.Session)

	requestInput := &s3manager.UploadInput{
		Bucket: aws.String(input.Bucket),
		Key:    aws.String(input.Item),
		Body:   input.File,
	}

	output, err := uploader.UploadWithContext(ctx, requestInput)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (svc *S3) CopyObject(input s3Domain.CopyObjectInput) error {
	source := input.SourceBucket + "/" + input.SourceItem
	_, err := svc.Client.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(input.DestinationBucket),
		CopySource: aws.String(url.PathEscape(source)),
		Key:        aws.String(input.DestinationBucketItem),
	})

	if err != nil {
		log.Fatalf("Unable to copy item from %q to %q/%q, %v", source, input.DestinationBucket, input.DestinationBucketItem, err)
		return err
	}

	err = svc.Client.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket: aws.String(input.DestinationBucket),
		Key:    aws.String(input.DestinationBucketItem),
	})
	if err != nil {
		log.Fatalf("Error occurred while waiting for item %q to be copied to %q/%q, %v", source, input.DestinationBucket, input.DestinationBucketItem, err)
		return err
	}

	fmt.Printf("Item %q successfully copied\n", input.DestinationBucketItem)
	return nil
}

func (svc *S3) CopyObjectWithContext(ctx context.Context, input s3Domain.CopyObjectInput) error {
	source := input.SourceBucket + "/" + input.SourceItem
	_, err := svc.Client.CopyObjectWithContext(ctx,
		&s3.CopyObjectInput{
			Bucket:     aws.String(input.DestinationBucket),
			CopySource: aws.String(url.PathEscape(source)),
			Key:        aws.String(input.DestinationBucketItem),
		})

	if err != nil {
		log.Fatalf("Unable to copy item from %q to %q/%q, %v", source, input.DestinationBucket, input.DestinationBucketItem, err)
		return err
	}

	err = svc.Client.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket: aws.String(input.DestinationBucket),
		Key:    aws.String(input.DestinationBucketItem),
	})
	if err != nil {
		log.Fatalf("Error occurred while waiting for item %q to be copied to %q/%q, %v", source, input.DestinationBucket, input.DestinationBucketItem, err)
		return err
	}

	fmt.Printf("Item %q successfully copied\n", input.DestinationBucketItem)
	return nil
}

func (svc *S3) DeleteObject(input s3Domain.DeleteObjectInput) error {
	_, err := svc.Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(input.Bucket),
		Key:    aws.String(input.Item),
	})

	if err != nil {
		log.Fatalf("Unable to delete object %q from bucket %q, %v", input.Item, input.Bucket, err)
		return err
	}

	err = svc.Client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(input.Bucket),
		Key:    aws.String(input.Item),
	})
	if err != nil {
		log.Fatalf("Error occurred while waiting for item %q to be deleted, %v", input.Item, err)
		return err
	}
	return nil
}

func (svc *S3) DeleteObjectWithContext(ctx context.Context, input s3Domain.DeleteObjectInput) error {
	_, err := svc.Client.DeleteObjectWithContext(ctx,
		&s3.DeleteObjectInput{
			Bucket: aws.String(input.Bucket),
			Key:    aws.String(input.Item),
		})

	if err != nil {
		log.Fatalf("Unable to delete object %q from bucket %q, %v", input.Item, input.Bucket, err)
		return err
	}

	err = svc.Client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(input.Bucket),
		Key:    aws.String(input.Item),
	})
	if err != nil {
		log.Fatalf("Error occurred while waiting for item %q to be deleted, %v", input.Item, err)
		return err
	}
	return nil
}

func (svc *S3) GetObjectUrl(bucket, key string) string {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	req, _ := svc.Client.GetObjectRequest(params)
	rest.Build(req)
	url := req.HTTPRequest.URL.String()
	return url
}
