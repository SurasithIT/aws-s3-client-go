package s3

import (
	"fmt"
	"io"
	"log"
	"net/url"

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

func (svc *S3) GetListObjects(bucket, prefix string) ([]*s3.Object, error) {

	requetObject := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	resp, err := svc.Client.ListObjectsV2(requetObject)
	if err != nil {
		log.Fatalf("Unable to list items in bucket %q, %v", bucket, err)
		return nil, err
	}

	return resp.Contents, nil
}

func (svc *S3) DownloadFile(bucket, item string) (*aws.WriteAtBuffer, error) {
	downloader := s3manager.NewDownloader(svc.Session)

	requestInput := s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(item),
	}

	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(buf, &requestInput)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (svc *S3) UploadFile(file io.Reader, bucket, item string) (*s3manager.UploadOutput, error) {
	uploader := s3manager.NewUploader(svc.Session)

	requestInput := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(item),
		Body:   file,
	}

	output, err := uploader.Upload(requestInput)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func (svc *S3) CopyObject(sourceBucket, sourceItem, destinationBucket, destinationBucketItem string) error {
	source := sourceBucket + "/" + sourceItem
	_, err := svc.Client.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(destinationBucket),
		CopySource: aws.String(url.PathEscape(source)),
		Key:        aws.String(destinationBucketItem),
	})

	if err != nil {
		log.Fatalf("Unable to copy item from %q to %q/%q, %v", source, destinationBucket, destinationBucketItem, err)
		return err
	}

	err = svc.Client.WaitUntilObjectExists(&s3.HeadObjectInput{
		Bucket: aws.String(destinationBucket),
		Key:    aws.String(destinationBucketItem),
	})
	if err != nil {
		log.Fatalf("Error occurred while waiting for item %q to be copied to %q/%q, %v", source, destinationBucket, destinationBucketItem, err)
		return err
	}

	fmt.Printf("Item %q successfully copied\n", destinationBucketItem)
	return nil
}

func (svc *S3) DeleteObject(bucket, item string) error {
	_, err := svc.Client.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(item)})
	if err != nil {
		log.Fatalf("Unable to delete object %q from bucket %q, %v", item, bucket, err)
		return err
	}

	err = svc.Client.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(item),
	})
	if err != nil {
		log.Fatalf("Error occurred while waiting for item %q to be deleted, %v", item, err)
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
