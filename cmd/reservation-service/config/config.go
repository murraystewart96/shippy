package config

import "github.com/spf13/viper"

type Config struct {
	Address       string        `mapstructure:"address" validate:"required"`
	Redis         Redis         `mapstructure:"redis"`
	VesselService VesselService `mapstructure:"vessel_service"`
}

func (c *Config) Defaults() {
	viper.SetDefault("address", ":50051")
	c.Redis.Defaults()
	c.VesselService.Defaults()
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

type VesselService struct {
	Address string `mapstructure:"address" validate:"required"`
}

func (v *VesselService) Defaults() {
	viper.SetDefault("vessel_service.address", "vessel-service:50051")
}
