package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Database struct {
	DSN             string `mapstructure:"dsn"`
	MaxOpenConns    int    `mapstructure:"max_open_conns"`
	ConnMaxLifetime string `mapstructure:"conn_max_lifetime"`
}

type Table struct {
	Source     string   `mapstructure:"source"`
	Target     string   `mapstructure:"target"`
	Cursor     string   `mapstructure:"cursor"`
	KeyColumns []string `mapstructure:"key_columns"`
}

type Config struct {
	Source            Database `mapstructure:"source"`
	Target            Database `mapstructure:"target"`
	Tables            []Table  `mapstructure:"tables"`
	BatchSize         int      `mapstructure:"batch_size"`
	Parallelism       int      `mapstructure:"table_parallelism"`
	MaxParams         int      `mapstructure:"max_parameters"`
	NoKeyCursorBatch  int      `mapstructure:"no_key_cursor_batch"`
	CheckpointBatches int      `mapstructure:"checkpoint_every_batches"`
	MetricsFile       string   `mapstructure:"metrics_file"`
	StateFile         string   `mapstructure:"state_file"`
	FullStrategy      string   `mapstructure:"full_strategy"`
}

func Load(path string, overrides map[string]string) (Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("KBSYNC")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	v.SetDefault("batch_size", 1000)
	v.SetDefault("table_parallelism", 2)
	v.SetDefault("max_parameters", 30000)
	v.SetDefault("no_key_cursor_batch", 50)
	v.SetDefault("checkpoint_every_batches", 10)
	v.SetDefault("state_file", ".kbsync-state.json")
	v.SetDefault("full_strategy", "truncate")
	if err := v.BindEnv("source.dsn", "KBSYNC_SOURCE_DSN"); err != nil {
		return Config{}, fmt.Errorf("绑定源库环境变量: %w", err)
	}
	if err := v.BindEnv("target.dsn", "KBSYNC_TARGET_DSN"); err != nil {
		return Config{}, fmt.Errorf("绑定目标库环境变量: %w", err)
	}

	if err := v.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("读取配置文件 %q: %w", path, err)
	}
	for key, value := range overrides {
		if value != "" {
			v.Set(key, value)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("解析配置文件: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Source.DSN == "" || c.Target.DSN == "" {
		return fmt.Errorf("source.dsn 和 target.dsn 均不能为空")
	}
	if len(c.Tables) == 0 {
		return fmt.Errorf("至少配置一张同步表")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch_size 必须大于 0")
	}
	if c.Parallelism <= 0 {
		return fmt.Errorf("table_parallelism 必须大于 0")
	}
	if c.MaxParams <= 0 {
		return fmt.Errorf("max_parameters 必须大于 0")
	}
	if c.NoKeyCursorBatch <= 0 {
		return fmt.Errorf("no_key_cursor_batch 必须大于 0")
	}
	if c.FullStrategy != "truncate" && c.FullStrategy != "delete" && c.FullStrategy != "append" {
		return fmt.Errorf("full_strategy 只能是 truncate、delete 或 append")
	}
	for i, table := range c.Tables {
		if table.Source == "" {
			return fmt.Errorf("tables[%d].source 不能为空", i)
		}
		if table.Target == "" {
			c.Tables[i].Target = table.Source
		}
	}
	return nil
}
