package repository

import (
	"github.com/ta-anomaly-detection/web-server/internal/domain/entity"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type UserRepository struct {
	Repository[entity.User]
	Log *zap.Logger
}

func NewUserRepository(log *zap.Logger) *UserRepository {
	return &UserRepository{
		Log: log,
	}
}

func (r *UserRepository) FindByToken(db *gorm.DB, user *entity.User, token string) error {
	return db.Where("token = ?", token).First(user).Error
}
