package s3

type GetListObjectsInput struct {
	Bucket string `validate:"required"`
	Prefix string
}
