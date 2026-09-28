package e2bcompat

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3Config names the bucket that keeps COPY archives; credentials come from the default AWS chain.
type s3Config struct {
	Bucket         string
	Prefix         string
	Endpoint       string
	Region         string
	ForcePathStyle bool
}

// s3Uploads keeps archives in a bucket and hands the SDK presigned PUTs, so every replica reads the same uploads.
type s3Uploads struct {
	client  *awss3.Client
	presign *awss3.PresignClient
	bucket  string
	prefix  string
}

func newS3Uploads(ctx context.Context, cfg s3Config) (*s3Uploads, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	return &s3Uploads{client: client, presign: awss3.NewPresignClient(client), bucket: cfg.Bucket, prefix: path.Join(cfg.Prefix, "e2b-files")}, nil
}

func (u *s3Uploads) present(ctx context.Context, key uploadKey) (bool, error) {
	_, err := u.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &u.bucket, Key: u.key(key)})
	if _, ok := errors.AsType[*types.NotFound](err); ok {
		return false, nil
	}
	return err == nil, err
}

func (u *s3Uploads) uploadURL(r *http.Request, key uploadKey) (string, map[string]string, error) {
	req, err := u.presign.PresignPutObject(r.Context(), &awss3.PutObjectInput{Bucket: &u.bucket, Key: u.key(key)}, awss3.WithPresignExpires(uploadURLTTL))
	if err != nil {
		return "", nil, err
	}
	headers := map[string]string{}
	for k, v := range req.SignedHeader {
		if k != "Host" && len(v) > 0 {
			headers[k] = v[0]
		}
	}
	return req.URL, headers, nil
}

func (u *s3Uploads) open(ctx context.Context, key uploadKey) (io.ReadCloser, error) {
	out, err := u.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &u.bucket, Key: u.key(key)})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (u *s3Uploads) key(key uploadKey) *string {
	return aws.String(path.Join(u.prefix, key.path()))
}
