package middleware

import (
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

			fields := []zapcore.Field{
				zap.String("ip", c.RealIP()),
				zap.String("time", start.Format(time.RFC3339)),
				zap.String("method", req.Method),
				zap.String("responseTime", time.Since(start).String()),
				zap.String("url", req.RequestURI),
				zap.String("param", req.URL.RawQuery),
				zap.String("protocol", req.Proto),
				zap.Int("responseCode", res.Status),
				zap.Int64("responseByte", res.Size),
				zap.String("userAgent", req.UserAgent()),
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
