package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// traQにWebhookを送信する
func postWebhook(message string, webhookId string, webhookSecret string) error {
	webhookUrl := "https://q.trap.jp/api/v3/webhooks/" + webhookId

	// Webhookの署名を生成
	mac := hmac.New(sha1.New, []byte(webhookSecret))
	_, _ = mac.Write([]byte(message))
	sig := hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequest("POST", webhookUrl, strings.NewReader(message))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("X-TRAQ-Signature", sig)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	fmt.Printf("Sent webhook to traQ: statusCode: %d, body: %s\n", res.StatusCode, body)
	return nil
}
