// Package config loads and validates runtime configuration for the service.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contains runtime configuration for the ingestion API.
type Config struct {
	Env string

	HTTP  HTTPConfig
	Kafka KafkaConfig
}

// HTTPConfig contains HTTP server configuration.
type HTTPConfig struct {
	Addr              string
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	MaxBodyBytes      int64
}

// KafkaConfig contains Kafka producer configuration.
type KafkaConfig struct {
	Brokers      []string
	TopicLogsRaw string
}

// Load reads configuration from environment variables and validates it.
func Load() (Config, error) {
	var cfg Config

	cfg.Env = getOptional("APP_ENV", "local")

	httpAddr := getOptional("HTTP_ADDR", ":8081")

	readTimeout, err := durationOptional("HTTP_READ_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	writeTimeout, err := durationOptional("HTTP_WRITE_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	idleTimeout, err := durationOptional("HTTP_IDLE_TIMEOUT", 60*time.Second)
	if err != nil {
		return Config{}, err
	}

	readHeaderTimeout, err := durationOptional("HTTP_READ_HEADER_TIMEOUT", 5*time.Second)
	if err != nil {
		return Config{}, err
	}

	maxBodyBytes, err := int64Optional("HTTP_MAX_BODY_BYTES", 1<<20) // 1 MiB
	if err != nil {
		return Config{}, err
	}

	brokersRaw, err := required("KAFKA_BROKERS")
	if err != nil {
		return Config{}, err
	}

	topicLogsRaw, err := required("KAFKA_TOPIC_LOGS_RAW")
	if err != nil {
		return Config{}, err
	}

	cfg.HTTP = HTTPConfig{
		Addr:              httpAddr,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
		MaxBodyBytes:      maxBodyBytes,
	}

	cfg.Kafka = KafkaConfig{
		Brokers:      parseKafkaBrokers(brokersRaw),
		TopicLogsRaw: topicLogsRaw,
	}

	if err := validate(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func required(key string) (string, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return "", fmt.Errorf("missing required environment variable %s", key)
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("environment variable %s cannot be empty", key)
	}

	return value, nil
}

func getOptional(key string, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}

	return value
}

func durationOptional(key string, fallback time.Duration) (time.Duration, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}

	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("invalid duration for %s: %w", key, err)
	}

	return parsed, nil
}

func int64Optional(key string, fallback int64) (int64, error) {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid integer for %s: %w", key, err)
	}

	return parsed, nil
}

func parseKafkaBrokers(value string) []string {
	rawBrokers := strings.Split(value, ",")
	brokers := make([]string, 0, len(rawBrokers))

	for _, broker := range rawBrokers {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return brokers
}

func validate(cfg Config) error {
	if len(cfg.Kafka.Brokers) == 0 {
		return errors.New("KAFKA_BROKERS must contain at least one broker")
	}

	if cfg.Kafka.TopicLogsRaw == "" {
		return errors.New("KAFKA_TOPIC_LOGS_RAW is required")
	}

	if cfg.HTTP.MaxBodyBytes <= 0 {
		return errors.New("HTTP_MAX_BODY_BYTES must be greater than zero")
	}

	return nil
}
