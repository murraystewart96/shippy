package config

import "github.com/spf13/viper"

type Config struct {
	Address       string        `mapstructure:"address" validate:"required"`
	Redis         Redis         `mapstructure:"redis"`
	DB            DB            `mapstructure:"database"`
	VesselService VesselService `mapstructure:"vessel_service"`
	KafkaProducer KafkaProducer `mapstructure:"kafka_producer"`
	KafkaConsumer KafkaConsumer `mapstructure:"kafka_consumer"`
}

func (c *Config) Defaults() {
	viper.SetDefault("address", ":50051")
	c.Redis.Defaults()
	c.DB.Defaults()
	c.VesselService.Defaults()
	c.KafkaProducer.Defaults()
	c.KafkaConsumer.Defaults()
}

type KafkaProducer struct {
	BootstrapServers string `mapstructure:"bootstrap_servers" validate:"required"`
	Acks             string `mapstructure:"acks" validate:"required"`
}

func (k *KafkaProducer) Defaults() {
	viper.SetDefault("kafka_producer.acks", "all")
}

type KafkaConsumer struct {
	BootstrapServers string `mapstructure:"bootstrap_servers" validate:"required"`
	GroupID          string `mapstructure:"group_id" validate:"required"`
	OffsetReset      string `mapstructure:"offset_reset" validate:"required"`
}

func (k *KafkaConsumer) Defaults() {
	viper.SetDefault("kafka_consumer.group_id", "reservation-service")
	viper.SetDefault("kafka_consumer.offset_reset", "earliest")
}

type Redis struct {
	Addr string `mapstructure:"addr" validate:"required"`

	// TODO - configure these
	Username string `mapstructure:"username" validate:""`
	Password string `mapstructure:"password" validate:""`
	DB       int    `mapstructure:"db" validate:""`
}

func (r *Redis) Defaults() {
	viper.SetDefault("redis.addr", ":6379")
}

type DB struct {
	Host     string `mapstructure:"host"     validate:"required"`
	Port     string `mapstructure:"port"     validate:"required"`
	Name     string `mapstructure:"name"     validate:"required"`
	User     string `mapstructure:"user"     validate:"required"`
	Password string `mapstructure:"password" validate:"omitempty"`
}

func (db *DB) Defaults() {
	viper.SetDefault("db.host", "localhost")
	viper.SetDefault("db.port", "5432")
	viper.SetDefault("db.name", "reservation")
	viper.SetDefault("db.user", "reservation")
	viper.SetDefault("db.password", "password")
}

type VesselService struct {
	Address string `mapstructure:"address" validate:"required"`
}

func (v *VesselService) Defaults() {
	viper.SetDefault("vessel_service.address", "vessel-service:50051")
}
