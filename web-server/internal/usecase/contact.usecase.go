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
	"gorm.io/gorm"
)

type ContactUseCase struct {
	DB                *gorm.DB
	Log               *zap.Logger
	Validate          *validator.Validate
	ContactRepository *repository.ContactRepository
}

func NewContactUseCase(db *gorm.DB, logger *zap.Logger, validate *validator.Validate,
	contactRepository *repository.ContactRepository) *ContactUseCase {
	return &ContactUseCase{
		DB:                db,
		Log:               logger,
		Validate:          validate,
		ContactRepository: contactRepository,
	}
}

func (c *ContactUseCase) Create(ctx context.Context, request *dto.CreateContactRequest) (*dto.ContactResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.With(zap.Error(err)).Error("error validating request body")
		return nil, echo.ErrBadRequest
	}

	contact := &entity.Contact{
		ID:        uuid.New().String(),
		FirstName: request.FirstName,
		LastName:  request.LastName,
		Email:     request.Email,
		Phone:     request.Phone,
		UserId:    request.UserId,
	}

	if err := c.ContactRepository.Create(tx, contact); err != nil {
		c.Log.With(zap.Error(err)).Error("error creating contact")
		return nil, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("error creating contact")
		return nil, echo.ErrInternalServerError
	}

	return converter.ContactToResponse(contact), nil
}

func (c *ContactUseCase) Update(ctx context.Context, request *dto.UpdateContactRequest) (*dto.ContactResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	contact := new(entity.Contact)
	if err := c.ContactRepository.FindByIdAndUserId(tx, contact, request.ID, request.UserId); err != nil {
		c.Log.With(zap.Error(err)).Error("error getting contact")
		return nil, echo.ErrNotFound
	}

	if err := c.Validate.Struct(request); err != nil {
		c.Log.With(zap.Error(err)).Error("error validating request body")
		return nil, echo.ErrBadRequest
	}

	contact.FirstName = request.FirstName
	contact.LastName = request.LastName
	contact.Email = request.Email
	contact.Phone = request.Phone

	if err := c.ContactRepository.Update(tx, contact); err != nil {
		c.Log.With(zap.Error(err)).Error("error updating contact")
		return nil, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("error updating contact")
		return nil, echo.ErrInternalServerError
	}

	return converter.ContactToResponse(contact), nil
}

func (c *ContactUseCase) Get(ctx context.Context, request *dto.GetContactRequest) (*dto.ContactResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.With(zap.Error(err)).Error("error validating request body")
		return nil, echo.ErrBadRequest
	}

	contact := new(entity.Contact)
	if err := c.ContactRepository.FindByIdAndUserId(tx, contact, request.ID, request.UserId); err != nil {
		c.Log.With(zap.Error(err)).Error("error getting contact")
		return nil, echo.ErrNotFound
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("error getting contact")
		return nil, echo.ErrInternalServerError
	}

	return converter.ContactToResponse(contact), nil
}

func (c *ContactUseCase) Delete(ctx context.Context, request *dto.DeleteContactRequest) error {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.With(zap.Error(err)).Error("error validating request body")
		return echo.ErrBadRequest
	}

	contact := new(entity.Contact)
	if err := c.ContactRepository.FindByIdAndUserId(tx, contact, request.ID, request.UserId); err != nil {
		c.Log.With(zap.Error(err)).Error("error getting contact")
		return echo.ErrNotFound
	}

	if err := c.ContactRepository.Delete(tx, contact); err != nil {
		c.Log.With(zap.Error(err)).Error("error deleting contact")
		return echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("error deleting contact")
		return echo.ErrInternalServerError
	}

	return nil
}

func (c *ContactUseCase) Search(ctx context.Context, request *dto.SearchContactRequest) ([]dto.ContactResponse, int64, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.With(zap.Error(err)).Error("error validating request body")
		return nil, 0, echo.ErrBadRequest
	}

	contacts, total, err := c.ContactRepository.Search(tx, request)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("error getting contacts")
		return nil, 0, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("error getting contacts")
		return nil, 0, echo.ErrInternalServerError
	}

	responses := make([]dto.ContactResponse, len(contacts))
	for i, contact := range contacts {
		responses[i] = *converter.ContactToResponse(&contact)
	}

	return responses, total, nil
}