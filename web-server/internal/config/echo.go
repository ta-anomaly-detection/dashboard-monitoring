package config

import (
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"
)

func NewEcho(config *viper.Viper) *echo.Echo {
	e := echo.New()
	return e
}

func NewErrorHandler() echo.HTTPErrorHandler {
	return func(err error, ctx echo.Context) {
		if he, ok := err.(*echo.HTTPError); ok {
			ctx.JSON(he.Code, map[string]string{
				"error": he.Message.(string),
			})
		} else {
			ctx.JSON(500, map[string]string{
				"error": "Internal Server Error",
			})
		}
		ctx.Logger().Error(err)
	}
}
