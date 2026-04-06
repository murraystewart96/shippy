package config

import "github.com/spf13/viper"

type Config struct {
	Address       string        `mapstructure:"address" validate:"required"`
	Redis         Redis         `mapstructure:"redis"`
	DB            DB            `mapstructure:"database"`
	VesselService VesselService `mapstructure:"vessel_service"`
	KafkaProducer KafkaProducer `mapstructure:"kafka_producer"`
	KafkaConsumer KafkaConsumer `mapstructure:"kafka_consumer"`
	Manager       Manager       `mapstructure:"manager"`
}

func (c *Config) Defaults() {
	viper.SetDefault("address", ":50051")
	c.Redis.Defaults()
	c.DB.Defaults()
	c.VesselService.Defaults()
	c.KafkaProducer.Defaults()
	c.KafkaConsumer.Defaults()
	c.Manager.Defaults()
}

type KafkaProducer struct {
	BootstrapServers string `mapstructure:"bootstrap_servers" validate:"required"`
	Acks             string `mapstructure:"acks" validate:"required"`
}

func (k *KafkaProducer) Defaults() {
	viper.SetDefault("kafka_producer.bootstrap_servers", "")
	viper.SetDefault("kafka_producer.acks", "all")
}

type KafkaConsumer struct {
	BootstrapServers string `mapstructure:"bootstrap_servers" validate:"required"`
	GroupID          string `mapstructure:"group_id" validate:"required"`
	OffsetReset      string `mapstructure:"offset_reset" validate:"required"`
}

func (k *KafkaConsumer) Defaults() {
	viper.SetDefault("kafka_consumer.bootstrap_servers", "")
	viper.SetDefault("kafka_consumer.group_id", "reservation-service")
	viper.SetDefault("kafka_consumer.offset_reset", "earliest")
}

type Redis struct {
	Addr               string `mapstructure:"addr" validate:"required"`
	ReservationTTL     int    `mapstructure:"reservation_ttl"`
	ReservationDataTTL int    `mapstructure:"reservation_data_ttl"`

	// TODO - configure these
	Username string `mapstructure:"username" validate:""`
	Password string `mapstructure:"password" validate:""`
	DB       int    `mapstructure:"db" validate:""`
}

func (r *Redis) Defaults() {
	viper.SetDefault("redis.addr", ":6379")
	viper.SetDefault("redis.reservation_ttl", 600)       // 10 minutes
	viper.SetDefault("redis.reservation_data_ttl", 1800) // 30 minutes
}

type DB struct {
	Host     string `mapstructure:"host"     validate:"required"`
	Port     string `mapstructure:"port"     validate:"required"`
	Name     string `mapstructure:"name"     validate:"required"`
	User     string `mapstructure:"user"     validate:"required"`
	Password string `mapstructure:"password" validate:"omitempty"`
}

func (db *DB) Defaults() {
	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", "5432")
	viper.SetDefault("database.name", "reservation")
	viper.SetDefault("database.user", "reservation")
	viper.SetDefault("database.password", "password")
}

type VesselService struct {
	Address string `mapstructure:"address" validate:"required"`
}

func (v *VesselService) Defaults() {
	viper.SetDefault("vessel_service.address", "vessel-service:50051")
}

type Manager struct {
	CleanupInterval int `mapstructure:"cleanup_interval"`
	OutboxInterval  int `mapstructure:"outbox_interval"`
}

func (m *Manager) Defaults() {
	viper.SetDefault("manager.cleanup_interval", 60)
	viper.SetDefault("manager.outbox_interval", 15)
}
