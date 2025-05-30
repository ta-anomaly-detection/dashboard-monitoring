package middleware

import (
	"bytes"
	"io"
	"time"

	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func ZapLogger(log *zap.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()

			err := next(c)
			if err != nil {
				c.Error(err)
			}

			req := c.Request()
			res := c.Response()

			bodyBytes, err := io.ReadAll(req.Body)
			if err != nil {
				bodyBytes = []byte("could not read body")
			}
			req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

			fields := []zapcore.Field{
				zap.String("remote_ip", c.RealIP()),
				zap.String("latency", time.Since(start).String()),
				zap.String("host", req.Host),
				zap.String("http_method", req.Method),
				zap.String("request_uri", req.RequestURI),
				zap.String("http_version", req.Proto),
				zap.Int("response_status", res.Status),
				zap.Int64("response_size", res.Size),
				zap.String("referrer", req.Referer()),
				zap.String("request_body", string(bodyBytes)),
				zap.String("request_time", start.Format(time.RFC3339)),
				zap.String("user_agent", req.UserAgent()),
			}

			auth := GetUser(c)

			if auth != nil {
				fields = append(fields,
					zap.String("user_id", auth.ID),
				)
			}

			id := req.Header.Get(echo.HeaderXRequestID)
			if id == "" {
				id = res.Header().Get(echo.HeaderXRequestID)
			}
			fields = append(fields, zap.String("request_id", id))

			n := res.Status
			switch {
			case n >= 500:
				log.With(zap.Error(err)).Error("Server error", fields...)
			case n >= 400:
				log.With(zap.Error(err)).Warn("Client error", fields...)
			case n >= 300:
				log.Info("Redirection", fields...)
			default:
				log.Info("Success", fields...)
			}

			return nil
		}
	}
}
