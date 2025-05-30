package http

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/ta-anomaly-detection/web-server/internal/delivery/http/middleware"
	"github.com/ta-anomaly-detection/web-server/internal/domain/dto"
	"github.com/ta-anomaly-detection/web-server/internal/usecase"
	"go.uber.org/zap"
)

type UserController struct {
	Log     *zap.Logger
	UseCase *usecase.UserUseCase
}

func NewUserController(useCase *usecase.UserUseCase, logger *zap.Logger) *UserController {
	return &UserController{
		Log:     logger,
		UseCase: useCase,
	}
}

func (c *UserController) Register(ctx echo.Context) error {
	request := new(dto.RegisterUserRequest)
	if err := ctx.Bind(request); err != nil {
		c.Log.Warn("Failed to parse request body", zap.Error(err))
		return echo.ErrBadRequest
	}

	response, err := c.UseCase.Create(ctx.Request().Context(), request)
	if err != nil {
		c.Log.Warn("Failed to register user", zap.Error(err))
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.UserResponse]{Data: response})
}

func (c *UserController) Login(ctx echo.Context) error {
	request := new(dto.LoginUserRequest)

	if err := ctx.Bind(request); err != nil {
		c.Log.Warn("Failed to parse request body", zap.Error(err))
		return echo.ErrBadRequest
	}

	response, err := c.UseCase.Login(ctx.Request().Context(), request)
	if err != nil {
		c.Log.Warn("Failed to login user", zap.Error(err))
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.UserResponse]{Data: response})
}

func (c *UserController) Current(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	request := &dto.GetUserRequest{
		ID: auth.ID,
	}

	response, err := c.UseCase.Current(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Warn("Failed to get current user")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.UserResponse]{Data: response})
}

func (c *UserController) Logout(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	request := &dto.LogoutUserRequest{
		ID: auth.ID,
	}

	response, err := c.UseCase.Logout(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Warn("Failed to logout user")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[bool]{Data: response})
}

func (c *UserController) Update(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	request := new(dto.UpdateUserRequest)
	if err := ctx.Bind(request); err != nil {
		c.Log.Warn("Failed to parse request body", zap.Error(err))
		return echo.ErrBadRequest
	}

	request.ID = auth.ID
	response, err := c.UseCase.Update(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Warn("Failed to update user")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.UserResponse]{Data: response})
}
