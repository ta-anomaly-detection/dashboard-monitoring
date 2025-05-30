package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func BuildDSN(viper *viper.Viper, forLibPQ bool) string {
	username := viper.GetString("database.username")
	password := viper.GetString("database.password")
	host := viper.GetString("database.host")
	port := viper.GetInt("database.port")
	database := viper.GetString("database.name")
	sslMode := viper.GetString("database.sslmode")
	timezone := viper.GetString("database.timezone")

	if forLibPQ {
		return fmt.Sprintf(
			"postgres://%s:%s@%s:%d/%s?sslmode=%s",
			username, password, host, port, database, sslMode,
		)
	}

	return fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=%s",
		host, username, password, database, port, sslMode, timezone,
	)
}

func NewDatabase(viper *viper.Viper, log *zap.Logger) *gorm.DB {
	idleConnection := viper.GetInt("database.pool.idle")
	maxConnection := viper.GetInt("database.pool.max")
	maxLifeTimeConnection := viper.GetInt("database.pool.lifetime")

	dsn := BuildDSN(viper, false)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.New(&zapWriter{Logger: log}, logger.Config{
			SlowThreshold:             time.Second * 5,
			IgnoreRecordNotFoundError: true,
			ParameterizedQueries:      true,
			LogLevel:                  logger.Info,
		}),
	})
	if err != nil {
		log.Fatal("failed to connect to database", zap.Error(err))
	}

	connection, err := db.DB()
	if err != nil {
		log.Fatal("failed to get database connection", zap.Error(err))
	}

	connection.SetMaxIdleConns(idleConnection)
	connection.SetMaxOpenConns(maxConnection)
	connection.SetConnMaxLifetime(time.Duration(maxLifeTimeConnection) * time.Second)

	return db
}

type zapWriter struct {
	Logger *zap.Logger
}

func (z *zapWriter) Printf(message string, args ...interface{}) {
	z.Logger.Info(fmt.Sprintf(message, args...))
}
