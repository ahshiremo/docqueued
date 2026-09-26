package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestService_SubmitBeforeStart(t *testing.T) {
	service, err := NewService(1, 1, ProcessWithContext)
	if err != nil {
		t.Fatalf("NewService() failed :%v", err)
	}

	job, err := service.Submit("hello world")
	if !errors.Is(err, ErrNotStarted) {
		t.Errorf("error = %v, want ErrNotStarted", err)
	}

	if job != (Job{}) {
		t.Errorf("job = %+v, want Job{}", job)
	}

	if len(service.jobs) != 0 || len(service.pending) != 0 {
		t.Errorf(
			"rejected submission left work: stored = %d, pending = %d",
			len(service.jobs),
			len(service.pending),
		)
	}
}

func TestService_StartTwice(t *testing.T) {
	service, err := NewService(1, 1, ProcessWithContext)
	if err != nil {
		t.Fatalf("NewService() failed :%v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() failed : %v", err)
	}

	t.Cleanup(func() {
		service.Cancel()
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelWait()

		if err := service.Wait(waitCtx); err != nil {
			t.Errorf("service cleanup failed: %v", err)
		}
	})

	if err := service.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("error = %v, want ErrAlreadyStarted", err)
	}
}

func TestService_StartAfterShutdown(t *testing.T) {
	service, err := NewService(1, 1, ProcessWithContext)
	if err != nil {
		t.Fatalf("NewService() failed :%v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() failed : %v", err)
	}

	t.Cleanup(func() {
		service.Cancel()

		waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelWait()

		if err := service.Wait(waitCtx); err != nil {
			t.Errorf("service cleanup failed: %v", err)
		}
	})

	service.BeginDrain()

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()

	if err := service.Wait(waitCtx); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	if err := service.Start(); !errors.Is(err, ErrAlreadyStarted) {
		t.Errorf("error = %v, want ErrAlreadyStarted", err)
	}
}

func TestService_WaitBeforeStart(t *testing.T) {
	service, err := NewService(1, 1, ProcessWithContext)
	if err != nil {
		t.Fatalf("NewService() failed: %v", err)
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()

	if err := service.Wait(waitCtx); !errors.Is(err, ErrNotStarted) {
		t.Errorf("error = %v, want ErrNotStarted", err)
	}
}

func TestService_WaitAfterShutdown(t *testing.T) {
	service := newRunningTestService(t, 1, 1, ProcessWithContext)

	service.BeginDrain()

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()

	if err := service.Wait(waitCtx); err != nil {
		t.Fatalf("initial Wait() failed: %v", err)
	}

	expiredWaitCtx, cancelExpiredWait := context.WithTimeout(context.Background(), 0)
	defer cancelExpiredWait()

	if err := service.Wait(expiredWaitCtx); err != nil {
		t.Errorf("Wait() after shutdown = %v, want nil", err)
	}
}

func TestService_WaitTimeoutDoesNotCancelWorkers(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	releaseWorker := sync.OnceFunc(func() {
		close(release)
	})

	processor := func(workerCtx context.Context, text string) (Result, error) {
		close(started)

		select {
		case <-release:
			return ProcessWithContext(workerCtx, text)
		case <-workerCtx.Done():
			return Result{}, workerCtx.Err()
		}
	}

	service := newRunningTestService(t, 1, 1, processor)
	t.Cleanup(releaseWorker)

	text := "hello world"
	submitted, err := service.Submit(text)
	if err != nil {
		t.Fatalf("Submit() failed: %v", err)
	}
	waitForTestSignal(t, started, "processor to start")
	service.BeginDrain()

	expiredWaitCtx, cancelExpiredWait := context.WithTimeout(context.Background(), 0)
	defer cancelExpiredWait()

	if err := service.Wait(expiredWaitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() error = %v, want DeadlineExceeded", err)
	}

	running, err := service.Get(submitted.ID)
	if err != nil {
		t.Fatalf("Get() while processing failed: %v", err)
	}
	wantRunning := Job{
		ID:     submitted.ID,
		Text:   text,
		Status: StatusRunning,
	}

	if running != wantRunning {
		t.Errorf("running job = %+v, want %+v", running, wantRunning)
	}

	releaseWorker()

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()

	if err := service.Wait(waitCtx); err != nil {
		t.Fatalf("Wait() after release failed: %v", err)
	}

	completed, err := service.Get(submitted.ID)
	if err != nil {
		t.Fatalf("Get() after processing failed: %v", err)
	}

	wantCompleted := Job{
		ID:     submitted.ID,
		Text:   text,
		Status: StatusCompleted,
		Result: Process(text),
	}

	if completed != wantCompleted {
		t.Errorf("completed job = %+v, want %+v", completed, wantCompleted)
	}
}
