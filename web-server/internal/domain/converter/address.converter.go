package converter

import (
	"github.com/ta-anomaly-detection/web-server/internal/domain/dto"
	"github.com/ta-anomaly-detection/web-server/internal/domain/entity"
)

func AddressToResponse(address *entity.Address) *dto.AddressResponse {
	return &dto.AddressResponse{
		ID:         address.ID,
		Street:     address.Street,
		City:       address.City,
		Province:   address.Province,
		PostalCode: address.PostalCode,
		Country:    address.Country,
		CreatedAt:  address.CreatedAt,
		UpdatedAt:  address.UpdatedAt,
	}
}
