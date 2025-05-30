package route

import (
	"github.com/labstack/echo/v4"
	"github.com/ta-anomaly-detection/web-server/internal/delivery/http"
)

type RouteConfig struct {
	App                 *echo.Echo
	UserController      *http.UserController
	ContactController   *http.ContactController
	AddressController   *http.AddressController
	AuthMiddleware      echo.MiddlewareFunc
	ZapLoggerMiddleware echo.MiddlewareFunc
}

func (c *RouteConfig) Setup() {
	c.App.Use(c.ZapLoggerMiddleware)
	c.SetupGuestRoute()
	c.SetupAuthRoute()
}

func (c *RouteConfig) SetupGuestRoute() {
	c.App.POST("/api/users", c.UserController.Register)
	c.App.POST("/api/users/_login", c.UserController.Login)
}

func (c *RouteConfig) SetupAuthRoute() {
	authGroup := c.App.Group("/api", c.AuthMiddleware)

	authGroup.DELETE("/users", c.UserController.Logout)
	authGroup.PATCH("/users/_current", c.UserController.Update)
	authGroup.GET("/users/_current", c.UserController.Current)

	authGroup.GET("/contacts", c.ContactController.List)
	authGroup.POST("/contacts", c.ContactController.Create)
	authGroup.PUT("/contacts/:contactId", c.ContactController.Update)
	authGroup.GET("/contacts/:contactId", c.ContactController.Get)
	authGroup.DELETE("/contacts/:contactId", c.ContactController.Delete)

	authGroup.GET("/contacts/:contactId/addresses", c.AddressController.List)
	authGroup.POST("/contacts/:contactId/addresses", c.AddressController.Create)
	authGroup.PUT("/contacts/:contactId/addresses/:addressId", c.AddressController.Update)
	authGroup.GET("/contacts/:contactId/addresses/:addressId", c.AddressController.Get)
	authGroup.DELETE("/contacts/:contactId/addresses/:addressId", c.AddressController.Delete)
}
