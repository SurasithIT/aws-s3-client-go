package config

import (
	"os"
	"poc/s3-client/config/aws"
)

type Config struct {
	AWS aws.Config
}

func New() (*Config, error) {
	var config Config
	config.AWS.Region = os.Getenv("AWS_REGION")
	config.AWS.AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	config.AWS.SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	config.AWS.BucketName = os.Getenv("AWS_BUCKET_NAME")

	return &config, nil
}
