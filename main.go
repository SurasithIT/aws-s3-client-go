package main

import (
	"bytes"
	"fmt"
	"os"
	"poc/s3-client/config"
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
	s3.GetListObjects(config.AWS.BucketName, prefixPath)

	// Get list of of objects in bucket
	s3.GetListObjects(config.AWS.BucketName, "")

	// Donwload file
	item := "Test/Path/test-file.csv"
	buf, err := s3.DownloadFile(config.AWS.BucketName, item)
	if err != nil {
		panic(err)
	}
	os.WriteFile("./test-download.csv", buf.Bytes(), 0644)

	// Upload file
	item = "Test/Path/test-upload.csv"
	reader := bytes.NewReader(buf.Bytes())
	_, err = s3.UploadFile(reader, config.AWS.BucketName, item)
	if err != nil {
		panic(err)
	}

	// Copy object
	sourceItem := "Test/Path/test-file.csv"
	destItem := "Test/Path/test-copy.csv"
	s3.CopyObject(config.AWS.BucketName, sourceItem, config.AWS.BucketName, destItem)

	// Get ObjectUrl
	url := s3.GetObjectUrl(config.AWS.BucketName, destItem)
	fmt.Println(url)

	// Delete object
	s3.DeleteObject(config.AWS.BucketName, destItem)
}
