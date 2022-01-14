package dcl

import (
	"context"
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/konrads/log-monitor-service/pkg/data"
	"github.com/konrads/log-monitor-service/pkg/model"
	"github.com/konrads/log-monitor-service/pkg/util"
	"github.com/sirupsen/logrus"
)

// Orchestration mechanism for goroutine driven log aggregation

func Orchestrate(ctx context.Context, config model.Config, in <-chan model.BatchEvent, db *sql.DB) {
	writer := data.NewMessageWriterImpl(db)
	flushChan := RunScheduler(ctx, config)
	persistChan := RunMessageBatcher(ctx, config, in, flushChan)
	RunPersister(ctx, writer, persistChan)
}

func OrchestrateFromEnv(ctx context.Context, config model.Config, in <-chan model.BatchEvent) {
	db, err := sql.Open(`mysql`, config.MysqlURL)
	if err != nil {
		log.Fatalf(`Failed to connect to DB`)
	}
	Orchestrate(ctx, config, in, db)
}

// Scheduler for issuing `flush` events.
func RunScheduler(ctx context.Context, config model.Config) <-chan struct{} {
	flush := make(chan struct{})
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// send flush message
				flush <- struct{}{}
				time.Sleep(config.FlushFreq)
				logrus.Info(`Tick...`)
			}
		}
	}(ctx)
	return flush
}

func RunMessageBatcher(ctx context.Context, config model.Config, in <-chan model.BatchEvent, flush <-chan struct{}) <-chan model.PersistEvent {
	batchedChan := make(chan model.PersistEvent)
	batchSize := int(config.BatchSize)
	go func(ctx context.Context, batchedChan chan<- model.PersistEvent) {
		backlog := []model.Message{}
		for {
			select {
			case <-ctx.Done():
				for len(backlog) > 0 {
					actBatchSize := util.Min(len(backlog), batchSize)
					logrus.WithField(`out_batch_size`, actBatchSize).Info(`Flushing batch`)
					batchedChan <- model.PersistEvent{
						Messages: backlog[:actBatchSize],
					}
					backlog = backlog[actBatchSize:]
				}
				logrus.Info(`Exiting message batcher`)
				return
			case event := <-in:
				// append to backlog
				logrus.WithField(`in_batch_size`, len(event.Messages)).Info(`Received messages`)
				backlog = append(backlog, event.Messages...)
			case <-flush:
				// FIXME: repetition from above
				for len(backlog) > 0 {
					actBatchSize := util.Min(len(backlog), batchSize)
					logrus.WithField(`out_batch_size`, actBatchSize).Info(`Flushing batch`)
					batchedChan <- model.PersistEvent{
						Messages: backlog[:actBatchSize],
					}
					backlog = backlog[actBatchSize:]
				}
			}
		}
	}(ctx, batchedChan)
	return batchedChan
}

// Persister process that stores asyn batches of messages
func RunPersister(ctx context.Context, writer data.MessageWriter, in <-chan model.PersistEvent) {
	go func(ctx context.Context, in <-chan model.PersistEvent) {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-in:
				logrus.WithField(`batch_size`, len(event.Messages)).Info(`Persisting batch`)
				if err := writer.WriteMessages(ctx, event.Messages); err != nil {
					logrus.Error(`Failed to write batch`, err)
				}
			}
		}
	}(ctx, in)
}
