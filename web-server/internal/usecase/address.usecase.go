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

type AddressUseCase struct {
	DB                *gorm.DB
	Log               *zap.Logger
	Validate          *validator.Validate
	AddressRepository *repository.AddressRepository
	ContactRepository *repository.ContactRepository
}

func NewAddressUseCase(db *gorm.DB, logger *zap.Logger, validate *validator.Validate,
	contactRepository *repository.ContactRepository, addressRepository *repository.AddressRepository) *AddressUseCase {
	return &AddressUseCase{
		DB:                db,
		Log:               logger,
		Validate:          validate,
		ContactRepository: contactRepository,
		AddressRepository: addressRepository,
	}
}

func (c *AddressUseCase) Create(ctx context.Context, request *dto.CreateAddressRequest) (*dto.AddressResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to validate request body")
		return nil, echo.ErrBadRequest
	}

	contact := new(entity.Contact)
	if err := c.ContactRepository.FindByIdAndUserId(tx, contact, request.ContactId, request.UserId); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find contact")
		return nil, echo.ErrNotFound
	}

	address := &entity.Address{
		ID:         uuid.NewString(),
		ContactId:  contact.ID,
		Street:     request.Street,
		City:       request.City,
		Province:   request.Province,
		PostalCode: request.PostalCode,
		Country:    request.Country,
	}

	if err := c.AddressRepository.Create(tx, address); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to create address")
		return nil, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("failed to commit transaction")
		return nil, echo.ErrInternalServerError
	}

	return converter.AddressToResponse(address), nil
}

func (c *AddressUseCase) Update(ctx context.Context, request *dto.UpdateAddressRequest) (*dto.AddressResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	if err := c.Validate.Struct(request); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to validate request body")
		return nil, echo.ErrBadRequest
	}

	contact := new(entity.Contact)
	if err := c.ContactRepository.FindByIdAndUserId(tx, contact, request.ContactId, request.UserId); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find contact")
		return nil, echo.ErrNotFound
	}

	address := new(entity.Address)
	if err := c.AddressRepository.FindByIdAndContactId(tx, address, request.ID, contact.ID); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find address")
		return nil, echo.ErrNotFound
	}

	address.Street = request.Street
	address.City = request.City
	address.Province = request.Province
	address.PostalCode = request.PostalCode
	address.Country = request.Country

	if err := c.AddressRepository.Update(tx, address); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to update address")
		return nil, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("failed to commit transaction")
		return nil, echo.ErrInternalServerError
	}

	return converter.AddressToResponse(address), nil
}

func (c *AddressUseCase) Get(ctx context.Context, request *dto.GetAddressRequest) (*dto.AddressResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	contact := new(entity.Contact)
	if err := c.ContactRepository.FindByIdAndUserId(tx, contact, request.ContactId, request.UserId); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find contact")
		return nil, echo.ErrNotFound
	}

	address := new(entity.Address)
	if err := c.AddressRepository.FindByIdAndContactId(tx, address, request.ID, request.ContactId); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find address")
		return nil, echo.ErrNotFound
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("failed to commit transaction")
		return nil, echo.ErrInternalServerError
	}

	return converter.AddressToResponse(address), nil
}

func (c *AddressUseCase) Delete(ctx context.Context, request *dto.DeleteAddressRequest) error {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	contact := new(entity.Contact)
	if err := c.ContactRepository.FindByIdAndUserId(tx, contact, request.ContactId, request.UserId); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find contact")
		return echo.ErrNotFound
	}

	address := new(entity.Address)
	if err := c.AddressRepository.FindByIdAndContactId(tx, address, request.ID, request.ContactId); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find address")
		return echo.ErrNotFound
	}

	if err := c.AddressRepository.Delete(tx, address); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to delete address")
		return echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("failed to commit transaction")
		return echo.ErrInternalServerError
	}

	return nil
}

func (c *AddressUseCase) List(ctx context.Context, request *dto.ListAddressRequest) ([]dto.AddressResponse, error) {
	tx := c.DB.WithContext(ctx).Begin()
	defer tx.Rollback()

	contact := new(entity.Contact)
	if err := c.ContactRepository.FindByIdAndUserId(tx, contact, request.ContactId, request.UserId); err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find contact")
		return nil, echo.ErrNotFound
	}

	addresses, err := c.AddressRepository.FindAllByContactId(tx, contact.ID)
	if err != nil {
		c.Log.With(zap.Error(err)).Error("failed to find addresses")
		return nil, echo.ErrInternalServerError
	}

	if err := tx.Commit().Error; err != nil {
		c.Log.With(zap.Error(err)).Error("failed to commit transaction")
		return nil, echo.ErrInternalServerError
	}

	responses := make([]dto.AddressResponse, len(addresses))
	for i, address := range addresses {
		responses[i] = *converter.AddressToResponse(&address)
	}

	return responses, nil
}