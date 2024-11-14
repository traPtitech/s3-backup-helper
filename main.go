package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cheggaaa/pb/v3"
	"github.com/golang/snappy"
	"github.com/joho/godotenv"
	"golang.org/x/sync/semaphore"
	"google.golang.org/api/option"
)

// S3設定
type s3ConfigStruct struct {
	Region         string
	EndPoint       string
	AccessKey      string
	SecretKey      string
	ForcePathStyle bool
	Bucket         string
}

var s3Config s3ConfigStruct

// GCP設定
type gcpConfigStruct struct {
	CredentialsPath  string
	ProjectID        string
	Region           string
	BucketNameSuffix string
}

var gcpConfig gcpConfigStruct

// Webhook設定
var webhookId string
var webhookSecret string

// 並列ダウンロード数
var palalellNum int64 = 5

func init() {
	// 環境変数の読み込み
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error: Failed to load .env file")
	}
	s3Config.EndPoint = os.Getenv("S3_ENDPOINT")
	s3Config.Region = os.Getenv("S3_REGION")
	s3Config.AccessKey = os.Getenv("S3_ACCESS_KEY")
	s3Config.SecretKey = os.Getenv("S3_SECRET_KEY")
	s3Config.ForcePathStyle = os.Getenv("S3_FORCE_PATH_STYLE") == "true"
	s3Config.Bucket = os.Getenv("S3_BUCKET")
	gcpConfig.CredentialsPath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	gcpConfig.ProjectID = os.Getenv("GCP_PROJECT_ID")
	gcpConfig.Region = os.Getenv("GCS_REGION")
	gcpConfig.BucketNameSuffix = os.Getenv("GCS_BUCKET_NAME_SUFFIX")
	webhookId = os.Getenv("WEBHOOK_ID")
	webhookSecret = os.Getenv("WEBHOOK_SECRET")
	palalellNum, err = strconv.ParseInt(os.Getenv("PALALELL_NUM"), 10, 64)
	if err != nil {
		log.Fatalf("Error: Failed to convert PALALELL_NUM to int: %v", err)
	}
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

	// バックアップ用GCSバケット作成
	fmt.Println("Target buckets:")
	gcsBucketName := s3Config.Bucket + gcpConfig.BucketNameSuffix
	gcsBucketClient := gcsClient.Bucket(gcsBucketName)
	gcsBucketAttr, err := gcsBucketClient.Attrs(ctx)
	// バケットが存在しない場合は作成
	if err == storage.ErrBucketNotExist {
		gcsNewBucketAttr := storage.BucketAttrs{
			StorageClass:      "COLDLINE",
			Location:          gcpConfig.Region,
			VersioningEnabled: true,
			// 90日でデータ削除
			Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{
				{
					Action:    storage.LifecycleAction{Type: "Delete"},
					Condition: storage.LifecycleCondition{AgeInDays: 90},
				},
			}},
		}
		if err := gcsBucketClient.Create(ctx, gcpConfig.ProjectID, &gcsNewBucketAttr); err != nil {
			log.Fatalf("Error: Failed to create GCS bucket: %v", err)
		} else {
			fmt.Printf(" - %v -> %v(Created)\n", s3Config.Bucket, gcsBucketName)
		}
	} else if err != nil {
		// その他のエラー
		log.Fatalf("Error: Failed to get GCS bucket attributes: %v", err)
	} else {
		// 既に存在している場合、バケットの状態を確認
		if gcsBucketAttr.StorageClass != "COLDLINE" {
			log.Fatalf("Error: Bucket storage class is not COLDLINE: %v", gcsBucketAttr.StorageClass)
		}
		if !gcsBucketAttr.VersioningEnabled {
			log.Fatalf("Error: Bucket versioning is not enabled")
		}
		fmt.Printf(" - %v -> %v(Already exists)\n", s3Config.Bucket, gcsBucketName)
	}

	// 改行
	fmt.Println()

	// バックアップ計測用変数
	backupStartTime := time.Now()
	totalObjects := 0
	totalErrors := 0
	executionLimit := semaphore.NewWeighted(palalellNum)

	// バックアップ
	fmt.Printf("Bucking up objects in %v to %v: ", s3Config.Bucket, gcsBucketName)

	// オブジェクト一覧を取得し、オブジェクト数を記録
	allObjects, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Config.Bucket),
	})
	if err != nil {
		log.Fatalf("Error: Failed to list objects: %v", err)
	}
	fmt.Printf("%d objects\n", len(allObjects.Contents))
	totalObjects += len(allObjects.Contents)

	// 並列処理用
	var wg sync.WaitGroup
	// 各オブジェクトについて、エラーを格納する
	var errs []error
	// プログレスバー
	bar := pb.StartNew(len(allObjects.Contents))

	// 並列処理開始
	for _, object := range allObjects.Contents {
		wg.Add(1)
		executionLimit.Acquire(ctx, 1)

		go func() {
			defer executionLimit.Release(1)
			defer wg.Done()

			errCh := make(chan error, 1)
			go func() {
				// GCS書き込み用オブジェクト作成
				gcsObjectWriter := gcsBucketClient.Object(*object.Key).NewWriter(ctx)

				// S3オブジェクトのダウンロード
				s3ObjectOutput, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(s3Config.Bucket),
					Key:    object.Key,
				})
				if err != nil {
					errCh <- err
					return
				}

				// Snappy圧縮してGCSにアップロード
				snappyWriter := snappy.NewBufferedWriter(gcsObjectWriter)
				defer snappyWriter.Close()
				if _, err := io.Copy(snappyWriter, s3ObjectOutput.Body); err != nil {
					errCh <- err
					return
				}

				snappyWriter.Flush()

				if err := gcsObjectWriter.Close(); err != nil {
					errCh <- err
					return
				}

				errCh <- nil
			}()

			if err := <-errCh; err != nil {
				log.Printf("Error: Failed to backup object %v: %v", *object.Key, err)
				errs = append(errs, err)
			}
		}()
		bar.Increment()
	}
	wg.Wait()
	bar.Finish()

	// エラー数をカウント
	totalErrors += len(errs)
	if len(errs) > 0 {
		fmt.Printf("Error: %d objects failed to backup\n", len(errs))
	}

	// バックアップ終了
	backupEndTime := time.Now()
	backupDuration := backupEndTime.Sub(backupStartTime)

	fmt.Printf("Backup completed: %d objects, %d errors, %v\n", totalObjects, totalErrors, backupDuration)

	// Webhook送信
	webhookMessage := fmt.Sprintf(`### オブジェクトストレージのバックアップが保存されました
	S3バケット: %s
	バックアップ開始時刻: %s
	バックアップ所要時間: %f時間
	オブジェクト数: %d
	エラー数: %d
	`, s3Config.Bucket, backupStartTime.Format("2006/01/02 15:04:05"), backupDuration.Hours(), totalObjects, totalErrors)
	postWebhook(webhookMessage, webhookId, webhookSecret)
}
