package dcl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/konrads/log-monitor-service/pkg/model"
	"github.com/stretchr/testify/assert"
)

func TestBatchWithFlush(t *testing.T) {
	config := model.Config{
		BatchSize: 2,
		FlushFreq: time.Second * 5,
		MysqlURL:  "NA",
	}

	ctx := context.Background()
	in := make(chan model.BatchEvent)
	flushChan := make(chan struct{})
	persistChan := RunMessageBatcher(ctx, config, in, flushChan)

	inBatch1 := []model.Message{
		{
			ServiceName: `s1`,
			Severity:    `debug`,
			Payload:     `p1`,
		},
		{
			ServiceName: `s2`,
			Severity:    `debug`,
			Payload:     `p1`,
		},
		{
			ServiceName: `s1`,
			Severity:    `debug`,
			Payload:     `p1`,
		},
	}

	inBatch2 := []model.Message{
		{
			ServiceName: `s1`,
			Severity:    `debug`,
			Payload:     `p1`,
		},
	}

	inBatch3 := []model.Message{
		{
			ServiceName: `s1`,
			Severity:    `debug`,
			Payload:     `p2`,
		},
	}

	inBatch4 := []model.Message{
		{
			ServiceName: `s1`,
			Severity:    `debug`,
			Payload:     `p3`,
		},
	}

	// add 3 messages in 1 incoming batch, expect 2 outgoing batches
	in <- model.BatchEvent{Messages: inBatch1}
	flushChan <- struct{}{}
	sleep()
	assertMessages(t, &persistChan, `failed to get 2 messages in batch1`, inBatch1[0], inBatch1[1])
	assertMessages(t, &persistChan, `failed to get 1 message in batch2`, inBatch1[2])
	assertMessages(t, &persistChan, `got unexpected batch`)

	// trigger flush with no new messages
	flushChan <- struct{}{}
	sleep()
	assertMessages(t, &persistChan, `got unexpected batch`)

	// add 3 single messages batches
	in <- model.BatchEvent{Messages: inBatch2}
	in <- model.BatchEvent{Messages: inBatch3}
	in <- model.BatchEvent{Messages: inBatch4}
	flushChan <- struct{}{}
	sleep()
	assertMessages(t, &persistChan, `failed to get 2 messages in batch3`, inBatch2[0], inBatch3[0])
	assertMessages(t, &persistChan, `failed to get 1 message in batch4`, inBatch4[0])
	assertMessages(t, &persistChan, `got unexpected batch`)
}

func assertMessages(t *testing.T, persistChan *<-chan model.PersistEvent, desc string, expMessages ...model.Message) {
	if len(expMessages) == 0 {
		select {
		case b := <-*persistChan:
			assert.Fail(t, fmt.Sprintf("%v: %v", desc, b))
		case <-time.After(time.Millisecond):
		}
	} else {
		select {
		case b := <-*persistChan:
			assert.Equal(t, model.PersistEvent{Messages: expMessages}, b, desc)
		case <-time.After(time.Millisecond):
			assert.Fail(t, desc)
		}
	}
}

func sleep() {
	time.Sleep(time.Millisecond)
}
