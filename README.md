# s3-backup-helper
 オブジェクトストレージのバックアップツール

 S3バケットごとにGCSバケットを作成し、各オブジェクトをSnappyで圧縮してアップロード、全て完了したらtraQにWebhookを送信します。

# 実行
## バックアップ
 ```go
 go run .
 ```

## 復元
 ```go
 go run restore/main.go
 ```
 `GCS_BUCKET`から`S3_BUCKET`に復元されます。  
 メタデータを修復するため、DBに接続する必要があります。

## 単一ファイル復元

 ```go
 go run decompress/main.go /path/to/snappy/file
 ```

# 設定
 `sample.env`から`.env`を作るか、環境変数で指定します。
 
 `GCS_BUCKET_NAME_SUFFIX`: GCSバケットが<S3バケット名> + `GCS_BUCKET_NAME_SUFFIX`という名前で作られます。  
 （GCSバケット名がグローバルでユニークである必要があるため）

 `PALALELL_NUM`: 同時に処理するオブジェクトの数
