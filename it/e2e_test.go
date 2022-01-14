///go:build it

package it

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/go-sql-driver/mysql"
	"github.com/konrads/log-monitor-service/cmd/dcl"
	"github.com/konrads/log-monitor-service/pkg/data"
	"github.com/konrads/log-monitor-service/pkg/model"
	"github.com/ory/dockertest/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var db *sql.DB
var dbUrl string

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool(``)
	if err != nil {
		log.Fatalf("Could not connect to docker: %v", err)
	}

	// switching to mariadb to enable M1 (Silicon) architecture on M1Pro
	resource, err := pool.Run(`mariadb`, `10.6`, []string{`MYSQL_ROOT_PASSWORD=secret`})
	if err != nil {
		log.Fatalf("Could not start resource: %v", err)
	}

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		dbUrl = fmt.Sprintf("root:secret@(localhost:%s)/mysql?parseTime=true", resource.GetPort(`3306/tcp`))
		db, err = sql.Open(`mysql`, dbUrl)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to database: %v", err)
	}

	// bootstrap db
	ctx := context.Background()
	_, err = db.ExecContext(ctx, `
	CREATE TABLE service_logs (
		service_name VARCHAR(100) NOT NULL,
		payload VARCHAR(2048) NOT NULL,
		severity ENUM("debug", "info", "warn", "error", "fatal") NOT NULL,
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
	)
	`)
	if err != nil {
		log.Fatalf("Failed to bootstrap db table service_logs: %s", err)
	}

	_, err = db.ExecContext(ctx, `
	CREATE TABLE service_severity (
		service_name VARCHAR(100) NOT NULL,
		severity ENUM("debug", "info", "warn", "error", "fatal") NOT NULL,
		count INT(4) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
		PRIMARY KEY (service_name, severity)
	)
	`)
	if err != nil {
		log.Fatalf("Failed to bootstrap db table service_severity: %v", err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %v", err)
	}

	os.Exit(code)
}

func TestDBInteractions(t *testing.T) {
	ctx := context.Background()

	inMessages := []model.Message{
		{
			ServiceName: `s1`,
			Payload:     `p`,
			Severity:    `debug`,
			Timestamp:   time.Now(),
		},
		{
			ServiceName: `s1`,
			Payload:     `p2`,
			Severity:    `debug`,
			Timestamp:   time.Now(),
		},
		{
			ServiceName: `s2`,
			Payload:     `p`,
			Severity:    `debug`,
			Timestamp:   time.Now(),
		},
	}

	expMessageSeverity := []model.MessageSeverity{
		{
			ServiceName: `s1`,
			Severity:    `debug`,
			Count:       2,
		},
		{
			ServiceName: `s2`,
			Severity:    `debug`,
			Count:       1,
		},
	}

	writer := data.NewMessageWriterImpl(db)
	err := writer.WriteMessages(ctx, inMessages)
	assert.NoError(t, err)

	reader := data.NewMessageReaderImpl(db)
	outMessages, err := reader.ReadAllMessages(ctx)
	assert.NoError(t, err)

	outSeverities, err := reader.ReadAllSeverities(ctx)
	assert.NoError(t, err)

	normalizeMessages(&inMessages)
	normalizeMessages(&outMessages)
	assert.Equal(t, inMessages, outMessages)
	assert.Equal(t, expMessageSeverity, outSeverities)
}

func Test2Minutes(t *testing.T) {
	// setup
	config := model.Config{
		BatchSize: 5000,
		FlushFreq: time.Minute,
		MysqlURL:  dbUrl,
	}
	in := make(chan model.BatchEvent)
	ctx := context.Background()
	dcl.Orchestrate(ctx, config, in, db)

	// generate a batch per second
	for i := 0; i < 125; i++ {
		in <- model.BatchEvent{Messages: getMessages(170)}
		time.Sleep(time.Second)
	}

	// validate db
	reader := data.NewMessageReaderImpl(db)
	outMessages, err := reader.ReadAllMessages(ctx)
	assert.NoError(t, err)

	outSeverities, err := reader.ReadAllSeverities(ctx)
	assert.NoError(t, err)

	logrus.Infof("Severities: %s\n", severitiesToString(outSeverities))

	assert.Greater(t, len(outMessages), 18_000)
	assert.Less(t, len(outMessages), 22_000)
	assert.Equal(t, len(outSeverities), 9)
}

func normalizeMessages(messages *[]model.Message) {
	ts := time.Unix(0, 0)
	for i := range *messages {
		(*messages)[i].Timestamp = ts
	}
}

func getRandom(arr ...string) string {
	randInd := gofakeit.Generate(fmt.Sprintf("{number:0,%d}", len(arr)-1))
	i, _ := strconv.Atoi(randInd)
	return arr[i]
}

func severitiesToString(severities []model.MessageSeverity) string {
	asStr := []string{}
	for _, m := range severities {
		asStr = append(asStr, fmt.Sprintf("- serviceName: %s, severity: %s, count: %d", m.ServiceName, m.Severity, m.Count))
	}
	return strings.Join(asStr, "\n")
}

func getMessages(howMany int) []model.Message {
	res := []model.Message{}
	for i := 0; i < howMany; i++ {
		res = append(res, model.Message{
			ServiceName: getRandom(`s1`, `s2`, `s3`),
			Severity:    getRandom(`info`, `debug`, `warn`),
			Payload:     gofakeit.Sentence(3),
			Timestamp:   time.Now(),
		})
	}
	return res
}
