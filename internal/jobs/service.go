package jobs

import (
	"errors"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrBlankText = errors.New("text must not be blank")
	ErrQueueFull = errors.New("queue is currently full")
	ErrNotFound  = errors.New("job not found")
)

type Service struct {
	mu        sync.Mutex
	jobs      map[string]Job
	pending   chan string
	nextId    uint64
	processor func(string) Result
}

func NewService(queueCapacity int, processor func(string) Result) (*Service, error) {
	if queueCapacity < 1 {
		return nil, errors.New("queue capacity must be positive")
	}

	if processor == nil {
		return nil, errors.New("processor must not be nil")
	}

	return &Service{
		jobs:      make(map[string]Job),
		pending:   make(chan string, queueCapacity),
		processor: processor,
	}, nil
}

func (s *Service) Submit(text string) (Job, error) {
	if strings.TrimSpace(text) == "" {
		return Job{}, ErrBlankText
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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

func (s *Service) ProcessNext() bool {
	var id string

	select {
	case id = <-s.pending:
	default:
		return false
	}

	s.mu.Lock()
	job := s.jobs[id]
	job.Status = StatusRunning
	s.jobs[id] = job
	s.mu.Unlock()

	result := s.processor(job.Text)

	s.mu.Lock()
	job.Result = result
	job.Status = StatusCompleted
	s.jobs[id] = job
	s.mu.Unlock()

	return true
}
