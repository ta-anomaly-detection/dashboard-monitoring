package middleware

import (
	"go.uber.org/zap"
	"github.com/labstack/echo/v4"
	"github.com/ta-anomaly-detection/web-server/internal/domain/dto"
	"github.com/ta-anomaly-detection/web-server/internal/usecase"
)

func NewAuth(userUseCase *usecase.UserUseCase) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ctx echo.Context) error {
			token := ctx.Request().Header.Get("Authorization")
			if token == "" {
				token = "NOT_FOUND"
			}

			userUseCase.Log.Debug("Authorization Header", zap.String("token", token))

			request := &dto.VerifyUserRequest{Token: token}
			auth, err := userUseCase.Verify(ctx.Request().Context(), request)
			if err != nil {
				userUseCase.Log.Warn("Failed to verify user", zap.Error(err))
				return echo.ErrUnauthorized
			}

			userUseCase.Log.Debug("User Authenticated", zap.String("user_id", auth.ID))
			ctx.Set("auth", auth)

			return next(ctx)
		}
	}
}

func GetUser(ctx echo.Context) *dto.Auth {
	auth, ok := ctx.Get("auth").(*dto.Auth)
	if !ok {
		return nil
	}
	return auth
}