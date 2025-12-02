package s3

import (
	"bytes"
	"context"
	"io"
	"log/slog"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func InitS3Client(endpoint string, accessKeyID string, secretAccessKey string, useSSL bool) *minio.Client {

	// Initialize minio client
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		panic(err)
	}

	return minioClient
}

func FetchObject(client *minio.Client, bucketName string, objectName string) ([]byte, error) {

	slog.Info("Fetching object from S3:", "bucket", bucketName, "object", objectName)

	object, err := client.GetObject(
		context.Background(),
		bucketName,
		objectName,
		minio.GetObjectOptions{},
	)

	if err != nil {
		slog.Error("Error requesting object from S3:", "error", err)
		return nil, err
	}

	defer object.Close()

	stat, err := object.Stat()
	if err != nil {
		slog.Error("Error fetching object stats from S3:", "error", err)
		return nil, err
	}

	slog.Info("Object info:", "size", stat.Size, "lastModified", stat.LastModified)

	data, err := io.ReadAll(object)

	if err != nil {
		slog.Error("Error reading object from S3:", "error", err)
		return nil, err
	}

	return data, nil
}

func UploadObject(client *minio.Client, bucketName string, objectName string, data []byte, contentType string) (minio.UploadInfo, error) {

	slog.Info("Uploading object to S3:", "bucket", bucketName, "object", objectName)

	info, err := client.PutObject(
		context.Background(),
		bucketName,
		objectName,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{ContentType: contentType},
	)

	return info, err
}
