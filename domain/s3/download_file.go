package s3

type DownloadFileInput struct {
	Bucket, Item string `validate:"required"`
}
