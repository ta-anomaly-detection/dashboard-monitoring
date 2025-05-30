package config

import (
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"
	"github.com/ta-anomaly-detection/web-server/internal/delivery/http"
	"github.com/ta-anomaly-detection/web-server/internal/delivery/http/middleware"
	"github.com/ta-anomaly-detection/web-server/internal/delivery/http/route"
	"github.com/ta-anomaly-detection/web-server/internal/repository"
	"github.com/ta-anomaly-detection/web-server/internal/usecase"
	"gorm.io/gorm"
)

type BootstrapConfig struct {
	DB       *gorm.DB
	App      *echo.Echo
	Log      *AppLoggers
	Validate *validator.Validate
	Config   *viper.Viper
}

func Bootstrap(config *BootstrapConfig) {
	// setup repositories
	userRepository := repository.NewUserRepository(config.Log.App)
	contactRepository := repository.NewContactRepository(config.Log.App)
	addressRepository := repository.NewAddressRepository(config.Log.App)

	// setup use cases
	userUseCase := usecase.NewUserUseCase(config.DB, config.Log.App, config.Validate, userRepository)
	contactUseCase := usecase.NewContactUseCase(config.DB, config.Log.App, config.Validate, contactRepository)
	addressUseCase := usecase.NewAddressUseCase(config.DB, config.Log.App, config.Validate, contactRepository, addressRepository)

	// setup controller
	userController := http.NewUserController(userUseCase, config.Log.App)
	contactController := http.NewContactController(contactUseCase, config.Log.App)
	addressController := http.NewAddressController(addressUseCase, config.Log.App)

	// setup middleware
	authMiddleware := middleware.NewAuth(userUseCase)
	zapLoggerMiddleware := middleware.ZapLogger(config.Log.Middleware)

	routeConfig := route.RouteConfig{
		App:               config.App,
		UserController:    userController,
		ContactController: contactController,
		AddressController: addressController,
		AuthMiddleware:    authMiddleware,
		ZapLoggerMiddleware: zapLoggerMiddleware,
	}
	routeConfig.Setup()
}
