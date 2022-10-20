package s3

type DeleteObjectInput struct {
	Bucket, Item string `validate:"required"`
}
