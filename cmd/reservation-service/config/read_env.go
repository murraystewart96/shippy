package config

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/go-playground/mold/v4/modifiers"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

const EnvPrefix = "SHIPPY_"

type environmentStruct interface {
	Defaults()
}

func ReadEnvironment[T environmentStruct](fullConfigPath string, vars T) {
	vars.Defaults()
	if fullConfigPath != "" {
		readFromFilePath(fullConfigPath)
	}

	viper.SetEnvPrefix(EnvPrefix)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	if err := unmarshal(vars); err != nil {
		log.Fatal().
			Str("full_config_path", fullConfigPath).
			Err(err).
			Msg("failed to unmarshal configuration")
	}
}

func readFromFilePath(fullConfigPath string) {
	configPath, configName := path.Split(fullConfigPath)
	viper.SetConfigName(configName)
	viper.AddConfigPath(configPath)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal().
			Err(err).
			Str("full_config_path", fullConfigPath).
			Msg("failed to read config file")
	}
}

func unmarshal[T any](vars T) error {
	if err := viper.Unmarshal(vars); err != nil {
		return fmt.Errorf("failed to unmarshall viper: %w", err)
	}

	// Run modifiers on config struct
	conform := modifiers.New()
	err := conform.Struct(context.Background(), vars)
	if err != nil {
		log.
			Warn().
			Err(err).
			Msg("An error occurred whilst conforming a config")
	}

	// Validate struct
	v := validator.New()

	if err := v.Struct(vars); err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}

	return nil
}
