package converter

import (
	"github.com/ta-anomaly-detection/web-server/internal/domain/dto"
	"github.com/ta-anomaly-detection/web-server/internal/domain/entity"
)

func ContactToResponse(contact *entity.Contact) *dto.ContactResponse {
	return &dto.ContactResponse{
		ID:        contact.ID,
		FirstName: contact.FirstName,
		LastName:  contact.LastName,
		Email:     contact.Email,
		Phone:     contact.Phone,
		CreatedAt: contact.CreatedAt,
		UpdatedAt: contact.UpdatedAt,
	}
}
