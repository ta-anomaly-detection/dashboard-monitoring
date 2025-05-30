package http

import (
	"math"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/ta-anomaly-detection/web-server/internal/delivery/http/middleware"
	"github.com/ta-anomaly-detection/web-server/internal/domain/dto"
	"github.com/ta-anomaly-detection/web-server/internal/usecase"
	"go.uber.org/zap"
)

type ContactController struct {
	UseCase *usecase.ContactUseCase
	Log     *zap.Logger
}

func NewContactController(useCase *usecase.ContactUseCase, log *zap.Logger) *ContactController {
	return &ContactController{
		UseCase: useCase,
		Log:     log,
	}
}

func (c *ContactController) Create(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	request := new(dto.CreateContactRequest)
	if err := ctx.Bind(request); err != nil {
		c.Log.With(zap.Error(err)).Error("error parsing request body")
		return echo.ErrBadRequest
	}
	request.UserId = auth.ID

	response, err := c.UseCase.Create(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("error creating contact")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.ContactResponse]{Data: response})
}

func (c *ContactController) List(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	page, _ := strconv.Atoi(ctx.QueryParam("page"))
	if page == 0 {
		page = 1
	}

	size, _ := strconv.Atoi(ctx.QueryParam("size"))
	if size == 0 {
		size = 10
	}

	request := &dto.SearchContactRequest{
		UserId: auth.ID,
		Name:   ctx.QueryParam("name"),
		Email:  ctx.QueryParam("email"),
		Phone:  ctx.QueryParam("phone"),
		Page:   page,
		Size:   size,
	}

	responses, total, err := c.UseCase.Search(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("error searching contact")
		return err
	}

	paging := &dto.PageMetadata{
		Page:      request.Page,
		Size:      request.Size,
		TotalItem: total,
		TotalPage: int64(math.Ceil(float64(total) / float64(request.Size))),
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[[]dto.ContactResponse]{
		Data:   responses,
		Paging: paging,
	})
}

func (c *ContactController) Get(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	request := &dto.GetContactRequest{
		UserId: auth.ID,
		ID:     ctx.Param("contactId"),
	}

	response, err := c.UseCase.Get(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("error getting contact")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.ContactResponse]{Data: response})
}

func (c *ContactController) Update(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)

	request := new(dto.UpdateContactRequest)
	if err := ctx.Bind(request); err != nil {
		c.Log.With(zap.Error(err)).Error("error parsing request body")
		return echo.ErrBadRequest
	}

	request.UserId = auth.ID
	request.ID = ctx.Param("contactId")

	response, err := c.UseCase.Update(ctx.Request().Context(), request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("error updating contact")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[*dto.ContactResponse]{Data: response})
}

func (c *ContactController) Delete(ctx echo.Context) error {
	auth := middleware.GetUser(ctx)
	contactId := ctx.Param("contactId")

	request := &dto.DeleteContactRequest{
		UserId: auth.ID,
		ID:     contactId,
	}

	if err := c.UseCase.Delete(ctx.Request().Context(), request); err != nil {
		c.Log.With(zap.Error(err)).Error("error deleting contact")
		return err
	}

	return ctx.JSON(http.StatusOK, dto.WebResponse[bool]{Data: true})
}