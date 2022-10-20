package s3

type CopyObjectInput struct {
	SourceBucket, SourceItem, DestinationBucket, DestinationBucketItem string `validate:"required"`
}
