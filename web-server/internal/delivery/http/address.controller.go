package http

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/ta-anomaly-detection/web-server/internal/delivery/http/middleware"
	"github.com/ta-anomaly-detection/web-server/internal/domain/dto"
	"github.com/ta-anomaly-detection/web-server/internal/usecase"
	"go.uber.org/zap"
)

type AddressController struct {
	UseCase *usecase.AddressUseCase
	Log     *zap.Logger
}

func NewAddressController(useCase *usecase.AddressUseCase, log *zap.Logger) *AddressController {
	return &AddressController{
		Log:     log,
		UseCase: useCase,
	}
}

func (c *AddressController) Create(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	request := new(dto.CreateAddressRequest)
	if err := ctx.Bind(request); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to parse request body")
		return echo.ErrBadRequest
	}

	request.UserId = auth.ID
	request.ContactId = ctx.Param("contactId")

	response, err := c.UseCase.Create(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("failed to create address")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.AddressResponse]{Data: response})
}

func (c *AddressController) List(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)
	contactId := ctx.Param("contactId")

	request := &dto.ListAddressRequest{
		UserId:    auth.ID,
		ContactId: contactId,
	}

	responses, err := c.UseCase.List(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("failed to list addresses")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[[]dto.AddressResponse]{Data: responses})
}

func (c *AddressController) Get(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)
	contactId := ctx.Param("contactId")
	addressId := ctx.Param("addressId")

	request := &dto.GetAddressRequest{
		UserId:    auth.ID,
		ContactId: contactId,
		ID:        addressId,
	}

	response, err := c.UseCase.Get(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("failed to get address")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.AddressResponse]{Data: response})
}

func (c *AddressController) Update(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	request := new(dto.UpdateAddressRequest)
	if err := ctx.Bind(request); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to parse request body")
		return echo.ErrBadRequest
	}

	request.UserId = auth.ID
	request.ContactId = ctx.Param("contactId")
	request.ID = ctx.Param("addressId")

	response, err := c.UseCase.Update(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("failed to update address")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.AddressResponse]{Data: response})
}

func (c *AddressController) Delete(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)
	contactId := ctx.Param("contactId")
	addressId := ctx.Param("addressId")

	request := &dto.DeleteAddressRequest{
		UserId:    auth.ID,
		ContactId: contactId,
		ID:        addressId,
	}

	if err := c.UseCase.Delete(ctx.Request().Context(), request); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to delete address")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[bool]{Data: true})
}