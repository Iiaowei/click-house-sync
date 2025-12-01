// kafka 包提供 Kafka 生产者辅助方法。
package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	k "github.com/segmentio/kafka-go"
)

// Message 是对 kafka-go Message 的别名。
type Message = k.Message

// NewWriter 返回一个配置了 brokers、topic 与 batchSize 的 kafka-go Writer。
func NewWriter(brokers []string, topic string, batchSize int) *k.Writer {
	return &k.Writer{
		Addr:         k.TCP(brokers...),
		Topic:        topic,
		BatchSize:    batchSize,
		BatchTimeout: time.Second,
		RequiredAcks: k.RequireAll,
		Balancer:     &k.Hash{},
	}
}

// MessageFromMap 将 map 编码为 JSONEachRow 并生成 Kafka 消息。
func MessageFromMap(m map[string]any, key []byte) (k.Message, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return k.Message{}, err
	}
	return k.Message{Key: key, Value: b}, nil
}

// WriteBatch 一次性写入一批消息到 Kafka。
func WriteBatch(ctx context.Context, w *k.Writer, msgs []k.Message) error {
	return w.WriteMessages(ctx, msgs...)
}

// IsUnknownTopicOrPartition 判断错误是否为 Unknown Topic Or Partition，以便在创建后重试。
func IsUnknownTopicOrPartition(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "Unknown Topic Or Partition") || strings.Contains(s, "[3]")
}

// WriteBatchWithRetry 写入消息；当主题未就绪时重试，并在两次尝试间等待。
func WriteBatchWithRetry(ctx context.Context, brokers []string, topic string, w *k.Writer, msgs []k.Message, retries int) error {
	for i := 0; i <= retries; i++ {
		err := w.WriteMessages(ctx, msgs...)
		if err == nil {
			return nil
		}
		if !IsUnknownTopicOrPartition(err) {
			return err
		}
		_ = WaitTopicReady(brokers, topic, 5*time.Second)
		time.Sleep(time.Duration(200*(i+1)) * time.Millisecond)
	}
	return w.WriteMessages(ctx, msgs...)
}
