package helpers

import (
	"bytes"
	"compress/gzip"
	"go-etl/s3"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

func decompressData(data []byte) ([]byte, error) {

	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var out bytes.Buffer
	if _, err := io.Copy(&out, reader); err != nil {
		return nil, err
	}

	return out.Bytes(), nil
}

func fetchFeedMessageObjectFromS3(objectName string) ([]byte, error) {
	s3_client := s3.InitS3Client(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_ACCESS_KEY_ID"),
		os.Getenv("S3_SECRET_ACCESS_KEY"),
		true,
	)

	data, err := s3.FetchObject(s3_client, "gtfs-rt", objectName)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func deseriaizeGzFeedMessage(fileData []byte) (*FeedMessage, error) {

	fileBytes, err := decompressData(fileData)

	if err != nil {
		return nil, err
	}

	var msg FeedMessage
	if err := proto.Unmarshal(fileBytes, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func LoadFeedMessage(objectName string) (*FeedMessage, error) {
	data, err := fetchFeedMessageObjectFromS3(objectName)

	if err != nil {
		return nil, err
	}

	return deseriaizeGzFeedMessage(data)
}

func WriteEnrichedFeedMessageToS3(objectName string, data []byte) error {
	s3_client := s3.InitS3Client(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_ACCESS_KEY_ID"),
		os.Getenv("S3_SECRET_ACCESS_KEY"),
		true,
	)

	_, err := s3.UploadObject(s3_client, "gtfs-rt", objectName, data, "application/json")

	return err
}
