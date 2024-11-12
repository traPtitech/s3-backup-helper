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
}

var s3Config s3ConfigStruct

// GCP設定
var gcpCredentialsPath string
var gcpProjectID string
var gcsRegion string
var gcsBucketNameSuffix string

// 並列ダウンロード数
var palalellNum int64 = 5

func init() {
	// 環境変数の読み込み
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error: Failed to load .env file")
	}
	s3Config.EndPoint = os.Getenv("S3_ENDPOINT")
	s3Config.Region = os.Getenv("S3_REGION")
	s3Config.AccessKey = os.Getenv("S3_ACCESS_KEY")
	s3Config.SecretKey = os.Getenv("S3_SECRET_KEY")
	s3Config.ForcePathStyle = os.Getenv("S3_FORCE_PATH_STYLE") == "true"
	gcpCredentialsPath = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	gcpProjectID = os.Getenv("GCP_PROJECT_ID")
	gcsRegion = os.Getenv("GCS_REGION")
	gcsBucketNameSuffix = os.Getenv("GCS_BUCKET_NAME_SUFFIX")
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

	// バケット一覧の取得
	s3Buckets, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		log.Fatalf("Error: Failed to list buckets: %v", err)
	}

	// GCSクライアントの作成
	ctx := context.Background()
	gcsClient, err := storage.NewClient(ctx, option.WithCredentialsFile(gcpCredentialsPath))
	if err != nil {
		log.Fatalf("Error: Failed to create GCS client: %v", err)
	}

	// バックアップ用GCSバケット作成
	fmt.Println("Target buckets:")
	for _, s3Bucket := range s3Buckets.Buckets {
		gcsBucketName := *s3Bucket.Name + gcsBucketNameSuffix
		gcsBucketClient := gcsClient.Bucket(gcsBucketName)
		gcsBucketAttr, err := gcsBucketClient.Attrs(ctx)

		// バケットが存在しない場合は作成
		if err == storage.ErrBucketNotExist {
			gcsNewBucketAttr := storage.BucketAttrs{
				StorageClass:      "COLDLINE",
				Location:          gcsRegion,
				VersioningEnabled: true,
				// 90日でデータ削除
				Lifecycle: storage.Lifecycle{Rules: []storage.LifecycleRule{
					{
						Action:    storage.LifecycleAction{Type: "Delete"},
						Condition: storage.LifecycleCondition{AgeInDays: 90},
					},
				}},
			}
			if err := gcsBucketClient.Create(ctx, gcpProjectID, &gcsNewBucketAttr); err != nil {
				log.Fatalf("Error: Failed to create GCS bucket: %v", err)
			} else {
				fmt.Printf(" - %v -> %v(Created)\n", *s3Bucket.Name, gcsBucketName)
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
			fmt.Printf(" - %v -> %v(Already exists)\n", *s3Bucket.Name, gcsBucketName)
		}
	}

	// 改行
	fmt.Println()

	// バックアップ計測用変数
	backupStartTime := time.Now()
	totalObjects := 0
	totalErrors := 0
	executionLimit := semaphore.NewWeighted(palalellNum)

	// バックアップ
	for _, s3Bucket := range s3Buckets.Buckets {
		gcsBucketName := *s3Bucket.Name + gcsBucketNameSuffix
		gcsBucketClient := gcsClient.Bucket(gcsBucketName)

		fmt.Printf("Bucking up objects in %v to %v: ", *s3Bucket.Name, gcsBucketName)

		// オブジェクト一覧を取得し、オブジェクト数を記録
		allObjects, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket: s3Bucket.Name,
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
						Bucket: s3Bucket.Name,
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
	}

	// バックアップ終了
	backupEndTime := time.Now()
	backupDuration := backupEndTime.Sub(backupStartTime)

	fmt.Printf("Backup completed: %d objects, %d errors, %v\n", totalObjects, totalErrors, backupDuration)
}
