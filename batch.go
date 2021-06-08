package gobatch

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

type Job func(ctx context.Context, queue []interface{}) error

var log = logrus.WithField("component", "goBatcher")

//Batcher struct for batcher job.
type Batcher struct {
	BatchSize    uint
	MaxWait      time.Duration
	Job          Job
	MaxRetry     uint
	currentRetry uint
	queue        []interface{}
}

func NewBatcher(job Job) *Batcher {
	result := &Batcher{
		Job:       job,
		BatchSize: 10,
		MaxWait:   30 * time.Second,
		// Queue:     make([]interface{}, 20),
	}
	result.initQueue()

	return result
}

//Start start batch
func (b *Batcher) Start(ctx context.Context) (chan interface{}, error) {

	maxWait := time.NewTimer(b.MaxWait)
	streams := make(chan interface{})

	go func() {
		defer func() {
			close(streams)
			maxWait.Stop()

			b.runJob(ctx)
		}()

	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case item := <-streams:
				b.pushCheckTrigger(ctx, item)

			case <-maxWait.C:
				log.Debug("time is up.")
				b.runJob(ctx)
				maxWait.Reset(b.MaxWait)
			}
		}
		log.Info("Done.")
	}()

	return streams, nil
}

//pushCheckTrigger push data to queue, check if > max size, then trigger event or wait.
func (b *Batcher) pushCheckTrigger(ctx context.Context, item interface{}) {

	b.queue = append(b.queue, item)
	log.Info("item received. queue len ", len(b.queue))

	if (len(b.queue)) >= int(b.BatchSize) {
		log.Info("queue is full. going to run job")
		b.runJob(ctx)
	}
}

func (b *Batcher) runJob(ctx context.Context) {
	if len(b.queue) == 0 {
		log.Debug("queue is empty.")
		return
	}
	err := b.Job(ctx, b.queue)

	if err != nil {
		log.Error("job return error ", err)
		if b.currentRetry < b.MaxRetry {
			log.Infof("check retry setting, MaxRetry = %d, currentRetry = %d",
				b.MaxRetry, b.currentRetry)
		} else {
			log.Error("max retry reached. adandon queue len = ", len(b.queue))
		}

		b.currentRetry = b.currentRetry + 1
	}

	if err == nil || b.currentRetry > b.MaxRetry {
		b.initQueue()
	}

}

func (b *Batcher) initQueue() {
	log.Info("batch job queue reset.")
	b.currentRetry = 0
	b.queue = []interface{}{}
}
