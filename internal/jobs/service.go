package jobs

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrBlankText      = errors.New("text must not be blank")
	ErrQueueFull      = errors.New("queue is currently full")
	ErrNotFound       = errors.New("job not found")
	ErrNotStarted     = errors.New("service has not started")
	ErrAlreadyStarted = errors.New("service has already started")
	ErrDraining       = errors.New("service is draining")
)

type Service struct {
	mu sync.Mutex

	jobs    map[string]Job
	pending chan string
	nextId  uint64

	processor   Processor
	workerCount int

	started  bool
	draining bool
	cancel   context.CancelFunc
	done     chan struct{}
}

func NewService(queueCapacity int, workerCount int, processor Processor) (*Service, error) {
	if queueCapacity < 1 {
		return nil, errors.New("queue capacity must be positive")
	}
	if workerCount < 1 {
		return nil, errors.New("worker count must be positive")
	}

	if processor == nil {
		return nil, errors.New("processor must not be nil")
	}

	return &Service{
		jobs:        make(map[string]Job),
		pending:     make(chan string, queueCapacity),
		processor:   processor,
		workerCount: workerCount,
		done:        make(chan struct{}),
	}, nil
}

func (s *Service) Submit(text string) (Job, error) {
	blank := strings.TrimSpace(text) == ""

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.draining {
		return Job{}, ErrDraining
	}

	if !s.started {
		return Job{}, ErrNotStarted
	}

	if blank {
		return Job{}, ErrBlankText
	}

	s.nextId++
	job := Job{
		ID:     strconv.FormatUint(s.nextId, 10),
		Text:   text,
		Status: StatusQueued,
	}

	s.jobs[job.ID] = job

	select {
	case s.pending <- job.ID:
		return job, nil
	default:
		delete(s.jobs, job.ID)
		return Job{}, ErrQueueFull
	}

}

func (s *Service) Get(id string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}

	return job, nil
}

func (s *Service) processJob(ctx context.Context, id string) {
	s.mu.Lock()

	job := s.jobs[id]
	err := ctx.Err()

	if err == nil {
		job.Status = StatusRunning
		s.jobs[id] = job
	}

	s.mu.Unlock()

	var result Result
	if err == nil {
		result, err = s.processor(ctx, job.Text)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		job.Status = StatusFailed
		job.Result = Result{}
		job.Failure = FailureProcessing

		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			job.Failure = FailureCancelled
		}
	} else {
		job.Status = StatusCompleted
		job.Result = result
		job.Failure = ""
	}

	s.jobs[id] = job
}

func (s *Service) BeginDrain() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.draining {
		return
	}
	s.draining = true
	close(s.pending)

	if !s.started {
		close(s.done)
	}
}

func (s *Service) Cancel() {
	s.BeginDrain()

	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Service) Wait(ctx context.Context) error {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()

	if !started {
		return ErrNotStarted
	}

	select {
	case <-s.done:
		return nil
	default:
	}

	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}

}
