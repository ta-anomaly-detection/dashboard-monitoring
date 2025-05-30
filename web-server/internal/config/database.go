package config

import (
	"database/sql"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func CreateDatabase(viper *viper.Viper, log *zap.Logger) (*sql.DB, error) {
	dsn := BuildDSN(viper, true)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal("failed to connect to database", zap.Error(err))
		return nil, err
	}

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + viper.GetString("database.name"))

	defer db.Close()
	if err = db.Ping(); err != nil {
		log.Fatal("failed to ping database", zap.Error(err))
		return nil, err
	}

	if err = db.Close(); err != nil {
		log.Fatal("failed to close database connection", zap.Error(err))
		return nil, err
	}

	return db, nil
}