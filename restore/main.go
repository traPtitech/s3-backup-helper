package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/golang/snappy"
	"github.com/joho/godotenv"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// S3設定（バケットも含む）
type s3ConfigStruct struct {
	Region         string
	EndPoint       string
	AccessKey      string
	SecretKey      string
	Bucket         string
	ForcePathStyle bool
}

var s3Config s3ConfigStruct

// GCP設定
type gcpConfigStruct struct {
	CredentialsPath string
	ProjectID       string
	Region          string
	Bucket          string
}

var gcpConfig gcpConfigStruct

func init() {
	err := godotenv.Load("restore/.env")
	if err != nil {
		log.Fatal("Error: Failed to load .env file")
	}

	// 環境変数の読み込み
	s3Config.EndPoint = os.Getenv("S3_ENDPOINT")
	s3Config.Region = os.Getenv("S3_REGION")
	s3Config.Bucket = os.Getenv("S3_BUCKET")
	s3Config.AccessKey = os.Getenv("S3_ACCESS_KEY")
	s3Config.SecretKey = os.Getenv("S3_SECRET_KEY")
	s3Config.ForcePathStyle = true

	gcpConfig.CredentialsPath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	gcpConfig.ProjectID = os.Getenv("GCP_PROJECT_ID")
	gcpConfig.Region = os.Getenv("GCS_REGION")
	gcpConfig.Bucket = os.Getenv("GCS_BUCKET")
}

func main() {
	// S3クライアントの作成
	s3Credential := credentials.NewStaticCredentialsProvider(s3Config.AccessKey, s3Config.SecretKey, "")
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(s3Credential),
		config.WithRegion(s3Config.Region),
	)
	if err != nil {
		log.Fatalf("Error: Failed to load configuration: %v", err)
	}
	s3Client := s3.NewFromConfig(cfg, func(opt *s3.Options) {
		opt.UsePathStyle = s3Config.ForcePathStyle
		opt.BaseEndpoint = aws.String(s3Config.EndPoint)
	})

	// GCSクライアントの作成
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx, option.WithCredentialsFile(gcpConfig.CredentialsPath))
	if err != nil {
		log.Fatalf("Error: Failed to create GCS client: %v", err)
	}
	defer gcsClient.Close()

	// GCSバケットの取得、存在判定
	gcsBucket := gcsClient.Bucket(gcpConfig.Bucket)
	_, err = gcsBucket.Attrs(ctx)
	if err != nil {
		log.Fatalf("Error: Failed to get bucket attributes. Please check that the bucket exists: %v", err)
	}

	// バケットが存在しない場合は作成
	_, err = s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s3Config.Bucket),
	})
	if err != nil {
		_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(s3Config.Bucket),
		})
		if err != nil {
			log.Fatalf("Error: Failed to create bucket: %v", err)
		}
	}

	fmt.Println("Target bucket:")
	fmt.Printf(" - %s -> %s\n", gcpConfig.Bucket, s3Config.Bucket)

	// 改行
	fmt.Println()

	// 復元計測用変数
	//restoreStartTime := time.Now()

	fmt.Println("Restoring objects: ")

	// オブジェクトの取得
	allObjects := gcsBucket.Objects(ctx, nil)

	// オブジェクト数
	totalObjects := 0
	// エラー数
	totalError := 0
	// TODO: 並列処理
	// TODO: プログレスバー表示、cheggaaa/pbをイテレーターに対して使う方法が分からない or 使えない？

	for {
		// GCSオブジェクトの取得
		object, err := allObjects.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			log.Printf("Error: Failed to get object: %v", err)
			totalError++
			continue
		}
		totalObjects++
		fmt.Printf(" - %s\n", object.Name)
		gcsObjectReader, err := gcsBucket.Object(object.Name).NewReader(ctx)
		if err != nil {
			log.Printf("Error: Failed to get object reader: %v", err)
			totalError++
			continue
		}

		// snappy解凍してS3にアップロード
		snappyReader := snappy.NewReader(gcsObjectReader)
		s3Uploader := manager.NewUploader(s3Client)
		_, err = s3Uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s3Config.Bucket),
			Key:    aws.String(object.Name),
			Body:   snappyReader,
		})
		if err != nil {
			log.Printf("Error: Failed to put object: %v", err)
			totalError++
			continue
		}
	}

	// 復元終了
	//restoreEndTime := time.Now()
	//restoreDuration := restoreEndTime.Sub(restoreStartTime)

	fmt.Printf("Restore completed: %d objects, %d errors\n", totalObjects, totalError)
}
