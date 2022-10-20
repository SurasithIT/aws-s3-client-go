package s3

import (
	"io"
)

type UploadFileInput struct {
	File         io.Reader `validate:"required"`
	Bucket, Item string    `validate:"required"`
}
