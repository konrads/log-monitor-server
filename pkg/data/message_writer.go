package data

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/konrads/log-monitor-service/pkg/model"
	"github.com/sirupsen/logrus"
)

// Message and severity writer
// Inserts a batch in 1 go, calculates severities and inserts them, or if exist - bumps the counts.
// The message/severity write is done within a transaction

type stringTuple struct {
	x1, x2 string
}

type MessageWriter interface {
	WriteMessages(ctx context.Context, messages []model.Message) error
}

type MessageWriterImpl struct {
	db *sql.DB
}

func NewMessageWriterImpl(db *sql.DB) *MessageWriterImpl {
	return &MessageWriterImpl{db}
}

func (w *MessageWriterImpl) WriteMessages(ctx context.Context, messages []model.Message) error {
	now := time.Now()

	// start transaction
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// create batch
	logrus.WithField(`batch_size`, len(messages)).Info(`Persisting messages`)
	sqlPrefix := `INSERT INTO service_logs (service_name, payload, severity, timestamp, created_at) VALUES `
	sqlParamsTemplate := `(?, ?, ?, ?, ?)`
	sqlParamsStrs := []string{}
	sqlParams := []interface{}{}
	stats := map[stringTuple]int{}
	for _, m := range messages {
		sqlParamsStrs = append(sqlParamsStrs, sqlParamsTemplate)
		sqlParams = append(sqlParams, m.ServiceName)
		sqlParams = append(sqlParams, m.Payload)
		sqlParams = append(sqlParams, m.Severity)
		sqlParams = append(sqlParams, m.Timestamp)
		sqlParams = append(sqlParams, now)
		t := stringTuple{x1: m.ServiceName, x2: m.Severity}
		count := stats[t]
		stats[t] = count + 1
	}

	// insert batch
	_, err = tx.ExecContext(ctx, sqlPrefix+strings.Join(sqlParamsStrs, `, `), sqlParams...)
	if err != nil {
		tx.Rollback()
		return err
	}

	// insert severity stats
	logrus.WithField(`batch_size`, len(stats)).Info(`Persisting severity stats`)
	for k, v := range stats {
		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO service_severity (service_name, severity, count, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE count = count + ?`,
			k.x1, k.x2, v, now, v)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
