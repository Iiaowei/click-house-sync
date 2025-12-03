// kafka 包提供 Kafka 管理辅助方法。
package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	k "github.com/segmentio/kafka-go"
)

// CreateTopic 按指定分区数与副本因子创建 Kafka 主题。
func CreateTopic(brokers []string, topic string, partitions int, replication int) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers")
	}
	var conn *k.Conn
	var firstErr error
	for _, b := range brokers {
		c, err := k.Dial("tcp", b)
		if err == nil {
			conn = c
			break
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if conn == nil {
		if firstErr != nil {
			return firstErr
		}
		return fmt.Errorf("brokers unreachable")
	}
	defer conn.Close()
	if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) > 0 {
		return nil
	}
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	host := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	admin, err := k.Dial("tcp", host)
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.CreateTopics(k.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: replication}); err != nil {
		return err
	}
	return WaitTopicReady(brokers, topic, 10*time.Second)
}

// WaitTopicReady 等待所有 broker 对该主题返回分区信息（主题就绪）。
func WaitTopicReady(brokers []string, topic string, timeout time.Duration) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers")
	}
	deadline := time.Now().Add(timeout)
	ready := make(map[string]bool)
	for {
		for _, b := range brokers {
			if ready[b] {
				continue
			}
			conn, err := k.Dial("tcp", b)
			if err == nil {
				parts, err := conn.ReadPartitions(topic)
				conn.Close()
				if err == nil && len(parts) > 0 {
					ready[b] = true
				}
			}
		}
		if len(ready) == len(brokers) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic not ready: %s", topic)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// DeleteTopic 删除 Kafka 主题；若主题不存在则返回 nil。
func DeleteTopic(brokers []string, topic string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers")
	}
	var conn *k.Conn
	var firstErr error
	for _, b := range brokers {
		c, err := k.Dial("tcp", b)
		if err == nil {
			conn = c
			break
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if conn == nil {
		if firstErr != nil {
			return firstErr
		}
		return fmt.Errorf("brokers unreachable")
	}
	defer conn.Close()
	if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 0 {
		return nil
	}
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	host := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	admin, err := k.Dial("tcp", host)
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.DeleteTopics(topic); err != nil {
		// 如果主题不存在，认为删除成功
		if err == k.UnknownTopicOrPartition { // 保护：kafka-go 可能不导出该常量
			return nil
		}
	}
	// 等待删除完成：分区不可读
	deadline := time.Now().Add(10 * time.Second)
	for {
		parts, err := conn.ReadPartitions(topic)
		if err == nil && len(parts) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("topic not deleted: %s", topic)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ListTopicNames 返回集群中的主题名称列表（从首个可达 broker 读取分区元数据并去重）。
func ListTopicNames(brokers []string) ([]string, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers")
	}
	var firstErr error
	for _, b := range brokers {
		conn, err := k.Dial("tcp", b)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		parts, err := conn.ReadPartitions()
		conn.Close()
		if err != nil {
			return nil, err
		}
		names := map[string]struct{}{}
		for _, p := range parts {
			names[p.Topic] = struct{}{}
		}
		var out []string
		for n := range names {
			out = append(out, n)
		}
		return out, nil
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, fmt.Errorf("brokers unreachable")
}

// ReadTopicPartitions 读取指定主题的分区元数据（从首个可达 broker）。
func ReadTopicPartitions(brokers []string, topic string) ([]k.Partition, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("no brokers")
	}
	var firstErr error
	for _, b := range brokers {
		conn, err := k.Dial("tcp", b)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		parts, err := conn.ReadPartitions(topic)
		conn.Close()
		if err != nil {
			return nil, err
		}
		return parts, nil
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, fmt.Errorf("brokers unreachable")
}

// CountTopicMessages 统计指定主题的消息总数（各分区的 last-offset 减 first-offset 之和）。
func CountTopicMessages(brokers []string, topic string) (int64, error) {
	parts, err := ReadTopicPartitions(brokers, topic)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, p := range parts {
		host := net.JoinHostPort(p.Leader.Host, strconv.Itoa(p.Leader.Port))
		conn, err := k.DialLeader(context.Background(), "tcp", host, topic, p.ID)
		if err != nil {
			return 0, err
		}
		first, err := conn.ReadFirstOffset()
		if err != nil {
			conn.Close()
			return 0, err
		}
		last, err := conn.ReadLastOffset()
		conn.Close()
		if err != nil {
			return 0, err
		}
		if last >= first {
			total += int64(last - first)
		}
	}
	return total, nil
}
