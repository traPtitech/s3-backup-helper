# s3-backup-helper
 オブジェクトストレージのバックアップツール

 S3バケットごとにGCSバケットを作成し、各オブジェクトをSnappyで圧縮してアップロード、全て完了したらtraQにWebhookを送信する。

# 実行
 ```go
 go run .
 ```

 `decompress`フォルダに単一ファイル解凍用のコードがある。

 ```go
 go run decompress/main.go /path/to/snappy/file
 ```

# 設定
 `sample.env`から`.env`を作るか、環境変数で指定する。
 
 `GCS_BUCKET_NAME_SUFFIX`: GCSバケットが<S3バケット名> + `GCS_BUCKET_NAME_SUFFIX`という名前で作られる。  
 （GCSバケット名がグローバルでユニークである必要があるため）

 `PALALELL_NUM`: 同時に処理するオブジェクトの数
