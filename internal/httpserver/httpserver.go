package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type Health struct {
	Database      Pinger
	MQTTConnected func() bool
	OutboxPending func(ctx context.Context) (int, error)
}

func New(l *zap.Logger, health Health, routes ...func(fiber.Router)) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		ErrorHandler:          errorHandler(l),
		ReadTimeout:           10 * time.Second,
		WriteTimeout:          10 * time.Second,
		IdleTimeout:           60 * time.Second,
	})
	app.Use(recover.New(), accessLog(l))

	app.Get("/healthz", func(c *fiber.Ctx) error {
		status, code, checks := health.check(c.UserContext(), l)
		return c.Status(code).JSON(fiber.Map{"status": status, "checks": checks})
	})
	for _, register := range routes {
		register(app)
	}
	return app
}

func Serve(lc fx.Lifecycle, app *fiber.App, addr string, l *zap.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", addr)
			if err != nil {
				return err
			}
			go func() {
				if err := app.Listener(ln); err != nil {
					l.Error("http server stopped", zap.Error(err))
				}
			}()
			l.Info("http server listening", zap.String("addr", ln.Addr().String()))
			return nil
		},
		OnStop: func(ctx context.Context) error { return app.ShutdownWithContext(ctx) },
	})
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func errorHandler(l *zap.Logger) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		var (
			status  = http.StatusInternalServerError
			code    = "internal"
			message = "internal server error"
			verr    *tracking.ValidationError
			ferr    *fiber.Error
		)
		switch {
		case errors.As(err, &verr):
			status, code, message = http.StatusBadRequest, "invalid_request", verr.Msg
		case errors.Is(err, tracking.ErrNotFound):
			status, code, message = http.StatusNotFound, "not_found", "no location found for this vehicle"
		case errors.As(err, &ferr):
			status, message = ferr.Code, ferr.Message
			code = map[int]string{http.StatusNotFound: "not_found", http.StatusMethodNotAllowed: "method_not_allowed"}[ferr.Code]
			if code == "" {
				code = "http_error"
			}
		default:
			l.Error("request failed", zap.String("path", c.Path()), zap.Error(err))
		}
		var body errorBody
		body.Error.Code, body.Error.Message = code, message
		return c.Status(status).JSON(body)
	}
}

func accessLog(l *zap.Logger) fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		if err != nil {
			if herr := c.App().ErrorHandler(c, err); herr != nil {
				return herr
			}
		}
		level := zapcore.InfoLevel
		if c.Path() == "/healthz" {
			level = zapcore.DebugLevel
		}
		l.Log(level, "http request",
			zap.String("method", c.Method()),
			zap.String("path", c.Path()),
			zap.Int("status", c.Response().StatusCode()),
			zap.Duration("took", time.Since(start)))
		return nil
	}
}

func (h Health) check(parent context.Context, l *zap.Logger) (status string, code int, checks map[string]any) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	status, code, checks = "ok", http.StatusOK, map[string]any{}

	if h.Database != nil {
		if err := h.Database.Ping(ctx); err != nil {
			l.Warn("health check: database unreachable", zap.Error(err))
			return "unavailable", http.StatusServiceUnavailable, map[string]any{"database": "down"}
		}
		checks["database"] = "ok"
	}
	if h.MQTTConnected != nil {
		if h.MQTTConnected() {
			checks["mqtt"] = "connected"
		} else {
			checks["mqtt"] = "disconnected"
			status = "degraded"
		}
	}
	if h.OutboxPending != nil {
		if n, err := h.OutboxPending(ctx); err != nil {
			checks["outbox_pending"] = "unknown"
			status = "degraded"
		} else {
			checks["outbox_pending"] = n
		}
	}
	return status, code, checks
}
