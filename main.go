package main

import (
	"context"
	"crypto/md5"
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
var webhookUrl string
var webhookId string
var webhookSecret string

// 並列ダウンロード数
var palalellNum int64 = 5

// フルバックアップかどうか
var fullBackup bool = false

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
	webhookUrl = os.Getenv("WEBHOOK_URL")
	webhookId = os.Getenv("WEBHOOK_ID")
	webhookSecret = os.Getenv("WEBHOOK_SECRET")
	palalellNum, err = strconv.ParseInt(os.Getenv("PALALELL_NUM"), 10, 64)
	if err != nil {
		log.Fatalf("Error: Failed to convert PALALELL_NUM to int: %v", err)
	}
	fullBackup = os.Getenv("FULL_BACKUP") == "true"
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
	skippedObjects := 0
	totalErrors := 0
	executionLimit := semaphore.NewWeighted(palalellNum)

	// バックアップ
	fmt.Printf("Bucking up objects in %v to %v\n", s3Config.Bucket, gcsBucketName)

	// オブジェクトのページネーターを作成
	objectPaginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Config.Bucket),
	})

	// 並列処理用
	var wg sync.WaitGroup
	// 各オブジェクトについて、エラーを格納する
	var errs []error

	// 並列処理開始
	for {
		if !objectPaginator.HasMorePages() {
			break
		}

		// オブジェクト取得
		page, err := objectPaginator.NextPage(ctx)
		if err != nil {
			log.Fatalf("Error: Failed to list objects: %v", err)
		}

		// プログレスバー
		bar := pb.StartNew(len(page.Contents))

		for _, object := range page.Contents {
			// 並列処理数を制限
			wg.Add(1)
			executionLimit.Acquire(ctx, 1)

			// オブジェクト数をカウント
			totalObjects++

			go func() {
				defer executionLimit.Release(1)
				defer wg.Done()

				errCh := make(chan error, 1)
				go func() {
					// S3オブジェクトのダウンロード
					s3ObjectOutput, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
						Bucket: aws.String(s3Config.Bucket),
						Key:    object.Key,
					})
					if err != nil {
						errCh <- err
						return
					}

					// フルバックアップでない場合、GCSオブジェクトとハッシュを比較
					if !fullBackup {
						// GCSオブジェクトの存在判定、情報取得
						gcsObjectAttrs, err := gcsBucketClient.Object(*object.Key).Attrs(ctx)
						// オブジェクトが存在する場合、ハッシュを比較
						if err == nil {
							s3Hash := md5.New()

							// ハッシュ計算
							hashWriter := snappy.NewBufferedWriter(s3Hash)
							defer hashWriter.Close()
							if _, err := io.Copy(hashWriter, s3ObjectOutput.Body); err != nil {
								errCh <- err
								return
							}
							hashWriter.Flush()

							// ハッシュを比較し、同じだったらスキップ
							if fmt.Sprintf("%x", gcsObjectAttrs.MD5) == fmt.Sprintf("%x", s3Hash.Sum(nil)) {
								skippedObjects++
								errCh <- nil
								return
							}
						}
					}

					// GCS書き込み用オブジェクト作成
					gcsObjectWriter := gcsBucketClient.Object(*object.Key).NewWriter(ctx)

					// メタデータ書き込み
					if s3ObjectOutput.ContentType != nil {
						gcsObjectWriter.ContentType = *s3ObjectOutput.ContentType
					}
					if s3ObjectOutput.ContentEncoding != nil {
						gcsObjectWriter.ContentEncoding = *s3ObjectOutput.ContentEncoding
					}
					if s3ObjectOutput.ContentDisposition != nil {
						gcsObjectWriter.ContentDisposition = *s3ObjectOutput.ContentDisposition
					}
					if s3ObjectOutput.ContentLanguage != nil {
						gcsObjectWriter.ContentLanguage = *s3ObjectOutput.ContentLanguage
					}
					if s3ObjectOutput.CacheControl != nil {
						gcsObjectWriter.CacheControl = *s3ObjectOutput.CacheControl
					}
					if s3ObjectOutput.Metadata != nil {
						if gcsObjectWriter.Metadata == nil {
							gcsObjectWriter.Metadata = make(map[string]string)
						}
						for key, value := range s3ObjectOutput.Metadata {
							gcsObjectWriter.Metadata[key] = value
						}
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
		bar.Finish()
		wg.Wait()
	}

	// エラー数をカウント
	totalErrors += len(errs)

	// バックアップ終了
	backupEndTime := time.Now()
	backupDuration := backupEndTime.Sub(backupStartTime)

	fmt.Printf("Backup completed: %d objects, %d skipped, %d errors, %v\n", totalObjects, skippedObjects, totalErrors, backupDuration)

	// Webhook送信
	webhookMessage := fmt.Sprintf(`### オブジェクトストレージのバックアップが保存されました
	S3バケット: %s
	バックアップ開始時刻: %s
	バックアップ所要時間: %f時間
	オブジェクト数: %d
	スキップされたオブジェクト数: %d
	エラー数: %d
	`, s3Config.Bucket, backupStartTime.Format("2006/01/02 15:04:05"), backupDuration.Hours(), totalObjects, skippedObjects, totalErrors)
	postWebhook(webhookMessage, webhookUrl, webhookId, webhookSecret)
}
