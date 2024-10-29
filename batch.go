package gobatch

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Job func(ctx context.Context, queue []interface{}) error

// var log = logrus.WithField("component", "goBatcher")

// Batcher struct for batcher job.
type Batcher struct {
	BatchSize    uint
	MaxWait      time.Duration
	Job          Job
	MaxRetry     uint
	currentRetry uint
	queue        []interface{}
	Logger       *zap.Logger
	mutex        sync.Mutex
}

func NewBatcher(job Job) *Batcher {
	result := &Batcher{
		Job:       job,
		BatchSize: 10,
		MaxWait:   30 * time.Second,
		Logger:    zap.L(),
		mutex:     sync.Mutex{},
	}

	return result
}

// Start start batch
func (b *Batcher) Start(ctx context.Context) (chan interface{}, error) {
	if b.Logger == nil {
		b.Logger = zap.L()
	}
	maxWait := time.NewTimer(b.MaxWait)
	streams := make(chan interface{}, b.BatchSize*10)

	b.initQueue()

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
				// b.Logger.Debug("time is up.")
				b.runJob(ctx)
				maxWait.Reset(b.MaxWait)
			}
		}
		b.Logger.Info("Done.")
	}()

	return streams, nil
}

// pushCheckTrigger push data to queue, check if > max size, then trigger event or wait.
func (b *Batcher) pushCheckTrigger(ctx context.Context, item interface{}) {

	b.queue = append(b.queue, item)
	b.Logger.Debug("item received. queue len ", zap.Int("len", len(b.queue)))

	if (len(b.queue)) >= int(b.BatchSize) {
		b.Logger.Info("queue is full. going to run job")
		b.runJob(ctx)
	}
}

func (b *Batcher) runJob(ctx context.Context) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(b.queue) == 0 {
		b.Logger.Debug("queue is empty, job done.")
		return
	}
	err := b.Job(ctx, b.queue)

	if err != nil {
		b.Logger.Error("job return error ", zap.Error(err))
		if b.currentRetry < b.MaxRetry {
			b.Logger.Info("check retry setting", zap.Uint("MaxRetry", b.MaxRetry), zap.Uint("currenty", b.currentRetry))
		} else {
			b.Logger.Error("max retry reached. adandon queue", zap.Int("len", len(b.queue)))
		}

		b.currentRetry = b.currentRetry + 1
	}

	if err == nil || b.currentRetry > b.MaxRetry {
		b.initQueue()
	}

}

func (b *Batcher) initQueue() {
	b.currentRetry = 0
	if b.queue == nil {
		b.queue = make([]interface{}, 0)
		b.Logger.Debug("batch job queue created.")
		return
	}

	if len(b.queue) > 0 {
		b.Logger.Debug("batch job queue reset.")
		b.Logger.Debug("cleanup queue")
		b.queue = nil
		b.queue = make([]interface{}, 0)
	} else {
		b.Logger.Debug("batch job queue is empty.")
	}
}
