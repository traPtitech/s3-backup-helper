package main

import (
	"io"
	"os"
	"path/filepath"

	"github.com/golang/snappy"
)

func main() {
	// 引数のパスのファイルを読み込む
	file, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	// 解凍先のファイルを作成
	newFileName := filepath.Base(os.Args[1]) + "_decompressed"
	newFile, err := os.Create(newFileName)
	if err != nil {
		panic(err)
	}
	defer newFile.Close()

	// snappyで解凍
	snappyReader := snappy.NewReader(file)
	_, err = io.Copy(newFile, snappyReader)
	if err != nil {
		panic(err)
	}
}
