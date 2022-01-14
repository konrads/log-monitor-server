package data

import (
	"context"
	"database/sql"

	"github.com/konrads/log-monitor-service/pkg/model"
)

type MessageReader interface {
	ReadAllMessages(context.Context) ([]model.Message, error)
	ReadAllStats(context.Context) ([]model.MessageSeverity, error)
}

type MessageReaderImpl struct {
	db *sql.DB
}

func NewMessageReaderImpl(db *sql.DB) MessageReaderImpl {
	return MessageReaderImpl{db}
}

func (r *MessageReaderImpl) ReadAllMessages(ctx context.Context) ([]model.Message, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT service_name, payload, severity, timestamp from service_logs ORDER BY service_name, payload, severity, timestamp`)
	if err != nil {
		return []model.Message{}, err
	}
	defer rows.Close()
	res := []model.Message{}
	for rows.Next() {
		m := model.Message{}
		err = rows.Scan(&m.ServiceName, &m.Payload, &m.Severity, &m.Timestamp)
		if err != nil {
			return []model.Message{}, err
		}
		res = append(res, m)
	}
	return res, nil
}

func (r *MessageReaderImpl) ReadAllSeverities(ctx context.Context) ([]model.MessageSeverity, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT service_name, severity, count from service_severity ORDER BY service_name, severity, count`)
	if err != nil {
		return []model.MessageSeverity{}, err
	}
	defer rows.Close()
	res := []model.MessageSeverity{}
	for rows.Next() {
		ms := model.MessageSeverity{}
		err = rows.Scan(&ms.ServiceName, &ms.Severity, &ms.Count)
		if err != nil {
			return []model.MessageSeverity{}, err
		}
		res = append(res, ms)
	}
	return res, nil
}
