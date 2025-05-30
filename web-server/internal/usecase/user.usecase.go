package usecase

import (
	"context"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"github.com/ta-anomaly-detection/web-server/internal/domain/dto"
	"github.com/ta-anomaly-detection/web-server/internal/domain/entity"
	"github.com/ta-anomaly-detection/web-server/internal/domain/converter"
	"github.com/ta-anomaly-detection/web-server/internal/repository"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type UserUseCase struct {
	DB             *gorm.DB
	Log            *zap.Logger
	Validate       *validator.Validate
	UserRepository *repository.UserRepository
}

func NewUserUseCase(db *gorm.DB, logger *zap.Logger, validate *validator.Validate,
	userRepository *repository.UserRepository) *UserUseCase {
	return &UserUseCase{
		DB:             db,
		Log:            logger,
		Validate:       validate,
		UserRepository: userRepository,
	}
}

func (c *UserUseCase) Verify(ctx context.Context, request *dto.VerifyUserRequest) (*dto.Auth, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	err := c.Validate.Struct(request)
	if err != nil {
		c.Log.Warn("Invalid request body", zap.Error(err))
		return nil, echo.ErrBadRequest
	}

	user := new(entity.User)
	if err := c.UserRepository.FindByToken(tx, user, request.Token); err != nil {
		c.Log.Warn("Failed find user by token", zap.Error(err))
		return nil, echo.ErrNotFound
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.Warn("Failed commit transaction", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	return &dto.Auth{ID: user.ID}, nil
}

func (c *UserUseCase) Create(ctx context.Context, request *dto.RegisterUserRequest) (*dto.UserResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	err := c.Validate.Struct(request)
	if err != nil {
		c.Log.Warn("Invalid request body", zap.Error(err))
		return nil, echo.ErrBadRequest
	}

	total, err := c.UserRepository.CountById(tx, request.ID)
	if err != nil {
		c.Log.Warn("Failed count user from database", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	if total > 0 {
		c.Log.Warn("User already exists")
		return nil, echo.ErrConflict
	}

	password, err := bcrypt.GenerateFromPassword([]byte(request.Password), bcrypt.DefaultCost)
	if err != nil {
		c.Log.Warn("Failed to generate bcrype hash", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	user := &entity.User{
		ID:       request.ID,
		Password: string(password),
		Name:     request.Name,
	}

	if err := c.UserRepository.Create(tx, user); err != nil {
		c.Log.Warn("Failed create user to database", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.Warn("Failed commit transaction", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	return converter.UserToResponse(user), nil
}

func (c *UserUseCase) Login(ctx context.Context, request *dto.LoginUserRequest) (*dto.UserResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.Warn("Invalid request body ", zap.Error(err))
		return nil, echo.ErrBadRequest
	}

	user := new(entity.User)
	if err := c.UserRepository.FindById(tx, user, request.ID); err != nil {
		c.Log.Warn("Failed find user by id", zap.Error(err))
		return nil, echo.ErrUnauthorized
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(request.Password)); err != nil {
		c.Log.Warn("Failed to compare user password with bcrype hash", zap.Error(err))
		return nil, echo.ErrUnauthorized
	}

	user.Token = uuid.New().String()
	if err := c.UserRepository.Update(tx, user); err != nil {
		c.Log.Warn("Failed save user", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.Warn("Failed commit transaction", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	return converter.UserToTokenResponse(user), nil
}

func (c *UserUseCase) Current(ctx context.Context, request *dto.GetUserRequest) (*dto.UserResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.Warn("Invalid request body", zap.Error(err))
		return nil, echo.ErrBadRequest
	}

	user := new(entity.User)
	if err := c.UserRepository.FindById(tx, user, request.ID); err != nil {
		c.Log.Warn("Failed find user by id", zap.Error(err))
		return nil, echo.ErrNotFound
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.Warn("Failed commit transaction", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	return converter.UserToResponse(user), nil
}

func (c *UserUseCase) Logout(ctx context.Context, request *dto.LogoutUserRequest) (bool, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.Warn("Invalid request body", zap.Error(err))
		return false, echo.ErrBadRequest
	}

	user := new(entity.User)
	if err := c.UserRepository.FindById(tx, user, request.ID); err != nil {
		c.Log.Warn("Failed find user by id", zap.Error(err))
		return false, echo.ErrNotFound
	}

	user.Token = ""

	if err := c.UserRepository.Update(tx, user); err != nil {
		c.Log.Warn("Failed save user", zap.Error(err))
		return false, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.Warn("Failed commit transaction", zap.Error(err))
		return false, echo.ErrInternalServerError
	}

	return true, nil
}

func (c *UserUseCase) Update(ctx context.Context, request *dto.UpdateUserRequest) (*dto.UserResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.Warn("Invalid request body", zap.Error(err))
		return nil, echo.ErrBadRequest
	}

	user := new(entity.User)
	if err := c.UserRepository.FindById(tx, user, request.ID); err != nil {
		c.Log.Warn("Failed find user by id", zap.Error(err))
		return nil, echo.ErrNotFound
	}

	if request.Name != "" {
		user.Name = request.Name
	}

	if request.Password != "" {
		password, err := bcrypt.GenerateFromPassword([]byte(request.Password), bcrypt.DefaultCost)
		if err != nil {
			c.Log.Warn("Failed to generate bcrype hash", zap.Error(err))
			return nil, echo.ErrInternalServerError
		}
		user.Password = string(password)
	}

	if err := c.UserRepository.Update(tx, user); err != nil {
		c.Log.Warn("Failed save user", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.Warn("Failed commit transaction", zap.Error(err))
		return nil, echo.ErrInternalServerError
	}

	return converter.UserToResponse(user), nil
}