package jobs

import (
	"context"
	"sync"
)

func (s *Service) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return ErrAlreadyStarted
	}
	if s.draining {
		return ErrDraining
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.started = true

	var workers sync.WaitGroup
	workers.Add(s.workerCount)
	for i := 0; i < s.workerCount; i++ {
		go func() {
			defer workers.Done()
			s.runWorker(ctx)
		}()
	}

	go func() {
		workers.Wait()
		cancel()
		close(s.done)
	}()

	return nil
}

func (s *Service) runWorker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case id, ok := <-s.pending:
			if !ok {
				return
			}
			s.processJob(ctx, id)
		}
	}
}
