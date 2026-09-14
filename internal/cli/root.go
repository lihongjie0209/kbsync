package cli

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"kbsync/internal/config"
	"kbsync/internal/progressui"
	"kbsync/internal/syncer"
)

var version = "dev"

type options struct {
	configFile string
	sourceDSN  string
	targetDSN  string
	logFormat  string
	logLevel   string
	progress   string
}

func New() *cobra.Command {
	var opts options
	root := &cobra.Command{
		Use:           "kbsync",
		Short:         "KingbaseES 到 KingbaseES 的结构与数据同步工具",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVarP(&opts.configFile, "config", "c", "kbsync.yaml", "配置文件路径")
	root.PersistentFlags().StringVar(&opts.sourceDSN, "source-dsn", "", "覆盖源库 DSN（也可用 KBSYNC_SOURCE_DSN）")
	root.PersistentFlags().StringVar(&opts.targetDSN, "target-dsn", "", "覆盖目标库 DSN（也可用 KBSYNC_TARGET_DSN）")
	root.PersistentFlags().StringVar(&opts.logFormat, "log-format", "text", "日志格式：text 或 json")
	root.PersistentFlags().StringVar(&opts.logLevel, "log-level", "info", "日志级别：debug、info、warn 或 error")
	root.PersistentFlags().StringVar(&opts.progress, "progress", "auto", "进度条：auto、always 或 never")

	root.AddCommand(newSyncCommand("full", "执行单次全量结构及数据同步", false, &opts))
	root.AddCommand(newSyncCommand("incremental", "从断点执行一次增量结构及数据同步", true, &opts))
	root.AddCommand(newSchemaCommand(&opts))
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "显示版本",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintln(cmd.OutOrStdout(), version)
			return err
		},
	})
	return root
}

func newSyncCommand(name, short string, incremental bool, opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   name,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opts, func(ctxRunner *syncer.Syncer) error {
				if incremental {
					return ctxRunner.Incremental(cmd.Context())
				}
				return ctxRunner.Full(cmd.Context())
			})
		},
	}
}

func newSchemaCommand(opts *options) *cobra.Command {
	return &cobra.Command{
		Use:   "schema",
		Short: "仅对比并同步表、列、主键、索引和关联序列",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, opts, func(ctxRunner *syncer.Syncer) error {
				return ctxRunner.Validate(cmd.Context(), false)
			})
		},
	}
}

func run(cmd *cobra.Command, opts *options, action func(*syncer.Syncer) error) (returnErr error) {
	cfg, err := config.Load(opts.configFile, map[string]string{
		"source.dsn": opts.sourceDSN,
		"target.dsn": opts.targetDSN,
	})
	if err != nil {
		return err
	}
	progressEnabled, progressForced, err := progressMode(opts.progress, opts.logFormat, cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	logOutput := cmd.ErrOrStderr()
	var reporter *progressui.Reporter
	if progressEnabled {
		reporter = progressui.New(cmd.ErrOrStderr(), progressForced)
		defer reporter.Finish()
		logOutput = reporter.LogWriter()
	}
	level := slog.LevelInfo
	if err := level.UnmarshalText([]byte(strings.ToUpper(opts.logLevel))); err != nil {
		return fmt.Errorf("无效 log-level %q: %w", opts.logLevel, err)
	}
	handlerOptions := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch opts.logFormat {
	case "text":
		handler = slog.NewTextHandler(logOutput, handlerOptions)
	case "json":
		handler = slog.NewJSONHandler(logOutput, handlerOptions)
	default:
		return fmt.Errorf("log-format 只能是 text 或 json")
	}
	structuredLogger := slog.New(handler)
	logger := func(format string, args ...any) {
		structuredLogger.InfoContext(cmd.Context(), fmt.Sprintf(format, args...))
	}
	runner, err := syncer.Open(cmd.Context(), cfg, logger)
	if err != nil {
		return err
	}
	if reporter != nil {
		runner.SetProgressReporter(reporter)
	}
	defer func() {
		if closeErr := runner.Close(); closeErr != nil && returnErr == nil {
			returnErr = fmt.Errorf("关闭数据库连接: %w", closeErr)
		}
	}()
	actionErr := action(runner)
	metricsErr := runner.WriteMetrics(cfg.MetricsFile)
	if actionErr != nil {
		return actionErr
	}
	return metricsErr
}

func progressMode(mode, logFormat string, output io.Writer) (enabled, forced bool, err error) {
	switch mode {
	case "never":
		return false, false, nil
	case "always":
		if logFormat == "json" {
			return false, false, fmt.Errorf("--progress=always 不能与 --log-format=json 同时使用")
		}
		return true, true, nil
	case "auto":
		if logFormat != "text" {
			return false, false, nil
		}
		file, ok := output.(*os.File)
		if !ok {
			return false, false, nil
		}
		info, statErr := file.Stat()
		if statErr != nil {
			return false, false, nil
		}
		return info.Mode()&os.ModeCharDevice != 0, false, nil
	default:
		return false, false, fmt.Errorf("progress 只能是 auto、always 或 never")
	}
}
