package main

import (
	"bytes"
	"fmt"
	"os"
	"poc/s3-client/config"
	s3Domain "poc/s3-client/domain/s3"
	"poc/s3-client/s3"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("./local.env")
	if err != nil {
		fmt.Printf("please consider environment variables: %s", err)
		panic(err)
	}

	config, err := config.New()
	if err != nil {
		panic(err)
	}

	s3 := s3.NewS3Client(config.AWS.AccessKey, config.AWS.SecretKey, "", &config.AWS.Region)

	prefixPath := "Test/Path"

	// Get list of of objects in bucket and prefix
	objects, err := s3.GetListObjects(s3Domain.GetListObjectsInput{
		Bucket: config.AWS.BucketName,
		Prefix: prefixPath,
	})
	if err != nil {
		panic(err)
	}

	for _, item := range objects {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}

	// Get list of of objects in bucket
	objects, err = s3.GetListObjects(s3Domain.GetListObjectsInput{
		Bucket: config.AWS.BucketName,
		Prefix: "",
	})
	if err != nil {
		panic(err)
	}

	for _, item := range objects {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}

	// Donwload file
	item := "Test/Path/test-file.csv"
	buf, err := s3.DownloadFile(s3Domain.DownloadFileInput{
		Bucket: config.AWS.BucketName,
		Item:   item,
	})
	if err != nil {
		panic(err)
	}
	os.WriteFile("./test-download.csv", buf.Bytes(), 0644)

	// Upload file
	item = "Test/Path/test-upload.csv"
	reader := bytes.NewReader(buf.Bytes())
	_, err = s3.UploadFile(s3Domain.UploadFileInput{
		File:   reader,
		Bucket: config.AWS.BucketName,
		Item:   item,
	})
	if err != nil {
		panic(err)
	}

	// Copy object
	sourceItem := "Test/Path/test-file.csv"
	destItem := "Test/Path/test-copy.csv"
	s3.CopyObject(s3Domain.CopyObjectInput{
		SourceBucket:          config.AWS.BucketName,
		SourceItem:            sourceItem,
		DestinationBucket:     config.AWS.BucketName,
		DestinationBucketItem: destItem,
	})

	// Get ObjectUrl
	url := s3.GetObjectUrl(config.AWS.BucketName, destItem)
	fmt.Println(url)

	// Delete object

	s3.DeleteObject(s3Domain.DeleteObjectInput{
		Bucket: config.AWS.BucketName,
		Item:   destItem,
	})
}
