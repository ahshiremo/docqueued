package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestService(t *testing.T, capacity, workers int, processor Processor) *Service {
	t.Helper()

	service, err := NewService(capacity, workers, processor)
	if err != nil {
		t.Fatalf("NewService() failed: %v", err)
	}

	if err := service.Start(); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	t.Cleanup(func() {
		service.Cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := service.Wait(ctx); err != nil {
			t.Errorf("service cleanup failed: %v", err)
		}
	})

	return service
}

func waitForTestSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestNewService(t *testing.T) {
	tests := []struct {
		name      string
		capacity  int
		workers   int
		processor Processor
		wantErr   bool
	}{
		{
			"valid configuration",
			2,
			1,
			ProcessWithContext,
			false,
		},
		{
			"zero capacity",
			0,
			1,
			ProcessWithContext,
			true,
		},
		{
			"negative capacity",
			-1,
			1,
			ProcessWithContext,
			true,
		},
		{
			name:      "zero workers",
			capacity:  2,
			workers:   0,
			processor: ProcessWithContext,
			wantErr:   true,
		},
		{
			name:      "negative workers",
			capacity:  2,
			workers:   -1,
			processor: ProcessWithContext,
			wantErr:   true,
		},
		{
			"nil processor",
			2,
			1,
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, err := NewService(tt.capacity, tt.workers, tt.processor)
			if tt.wantErr {
				if err == nil {
					t.Error("expected an error, got nil")
				}

				if service != nil {
					t.Error("expected a nil service for invalid configuration")
				}

				return
			}

			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}

			if service == nil {
				t.Fatal("expected a service, got nil")
			}

			if len(service.jobs) != 0 || len(service.pending) != 0 {
				t.Error("new service unexpectedly contains work")
			}
		})
	}
}

func TestService_Get(t *testing.T) {
	service := newTestService(t, 1, 1, ProcessWithContext)
	text := "hello world"

	submitted, err := service.Submit(text)
	if err != nil {
		t.Fatalf("Submit() failed: %v", err)
	}

	service.BeginDrain()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	want := Job{
		ID:     submitted.ID,
		Text:   text,
		Status: StatusCompleted,
		Result: Process(text),
	}

	snapshot, err := service.Get(submitted.ID)
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}

	if snapshot != want {
		t.Fatalf("Get() = %+v, want %+v", snapshot, want)
	}

	snapshot.Text = "changed by caller"
	snapshot.Status = StatusFailed
	snapshot.Failure = FailureProcessing
	snapshot.Result = Result{
		WordCount: 999,
		SHA256:    "changed",
	}

	stored, err := service.Get(submitted.ID)
	if err != nil {
		t.Fatalf("second Get() failed: %v", err)
	}

	if stored != want {
		t.Errorf("stored job changed: got %+v, want %+v", stored, want)
	}
}

func TestService_GetNotFound(t *testing.T) {
	service := newTestService(t, 1, 1, ProcessWithContext)
	job, err := service.Get("not found")

	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}

	if job != (Job{}) {
		t.Errorf("expected an empty job, recieved %v", job)
	}
}

func TestService_Submit(t *testing.T) {
	service := newTestService(t, 1, 1, ProcessWithContext)
	text := "\thello, world!\t"

	job, err := service.Submit(text)
	if err != nil {
		t.Fatalf("Submit() failed: %v", err)
	}

	if job.ID == "" {
		t.Fatal("expected a nonempty job ID")
	}

	want := Job{
		ID:     job.ID,
		Text:   text,
		Status: StatusQueued,
	}

	if job != want {
		t.Errorf("Submit() = %+v, want %+v", job, want)
	}
}

func TestService_SubmitWhitespaceOnly(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "spaces", text: "   "},
		{name: "mixed whitespace", text: " \t\n "},
		{name: "unicode whitespace", text: "\u2003"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := newTestService(t, 1, 1, ProcessWithContext)
			job, err := service.Submit(tt.text)

			if !errors.Is(err, ErrBlankText) {
				t.Errorf("error = %v, want ErrBlankText", err)
			}

			if job != (Job{}) {
				t.Errorf("job = %v, expected empty", job)
			}

			service.BeginDrain()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := service.Wait(ctx); err != nil {
				t.Fatalf("Wait() failed: %v", err)
			}

			if len(service.jobs) != 0 || len(service.pending) != 0 {
				t.Errorf("jobs length = %d, pending length = %d, expected 0", len(service.jobs), len(service.pending))
			}
		})
	}
}

func TestService_SubmitQueueFull(t *testing.T) {
	started := make(chan struct{})
	processor := func(ctx context.Context, _ string) (Result, error) {
		close(started)
		<-ctx.Done()
		return Result{}, ctx.Err()
	}
	service := newTestService(t, 1, 1, processor)

	first, err := service.Submit("document one")
	if err != nil {
		t.Fatalf("first Submit() failed: %v", err)
	}
	waitForTestSignal(t, started, "the worker to take the first job")

	second, err := service.Submit("document two")
	if err != nil {
		t.Fatalf("second Submit() failed: %v", err)
	}

	rejected, err := service.Submit("document three")

	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("error = %v, want ErrQueueFull", err)
	}

	if rejected != (Job{}) {
		t.Errorf("rejected job = %+v, want an empty Job", rejected)
	}

	service.mu.Lock()
	storedCount := len(service.jobs)
	service.mu.Unlock()

	if storedCount != 2 || len(service.pending) != 1 {
		t.Errorf(
			"stored=%d, pending=%d, want 2 stored and 1 pending",
			storedCount,
			len(service.pending),
		)
	}

	expected := []Job{
		{ID: first.ID, Text: "document one", Status: StatusRunning},
		{ID: second.ID, Text: "document two", Status: StatusQueued},
	}
	for _, want := range expected {
		stored, err := service.Get(want.ID)
		if err != nil {
			t.Fatalf("Get(%q) failed: %v", want.ID, err)
		}
		if stored != want {
			t.Errorf("accepted job = %+v, want %+v", stored, want)
		}
	}
}

func TestService_Processing(t *testing.T) {
	text := "hello\tworld!"
	result := Result{
		WordCount: 2,
		SHA256:    "test-digest",
	}

	var received string
	var calls int

	processor := func(_ context.Context, input string) (Result, error) {
		received = input
		calls++
		return result, nil
	}

	service := newTestService(t, 1, 1, processor)

	submitted, err := service.Submit(text)
	if err != nil {
		t.Fatalf("Submit() failed: %v", err)
	}

	service.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	completed, err := service.Get(submitted.ID)
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}

	want := Job{
		ID:     submitted.ID,
		Text:   text,
		Status: StatusCompleted,
		Result: result,
	}

	if completed != want {
		t.Errorf("completed job = %+v, want %+v", completed, want)
	}

	if received != text {
		t.Errorf("processor received %q, want %q", received, text)
	}

	if calls != 1 {
		t.Errorf("processor calls = %d, want 1", calls)
	}
}

func TestService_QueueCapacityReused(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})

	processor := func(ctx context.Context, text string) (Result, error) {
		switch text {
		case "document one":
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return Result{}, ctx.Err()
			}
		case "document two":
			close(secondStarted)
			<-ctx.Done()
			return Result{}, ctx.Err()
		}

		return ProcessWithContext(ctx, text)
	}

	service := newTestService(t, 1, 1, processor)
	first, err := service.Submit("document one")
	if err != nil {
		t.Fatalf("first Submit() failed: %v", err)
	}
	waitForTestSignal(t, firstStarted, "the first job to start")

	second, err := service.Submit("document two")
	if err != nil {
		t.Fatalf("second Submit() failed: %v", err)
	}

	if _, err := service.Submit("document three"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("error before freeing capacity = %v, want ErrQueueFull", err)
	}

	close(releaseFirst)
	waitForTestSignal(t, secondStarted, "the worker to finish the first job and take the second")

	third, err := service.Submit("document three")
	if err != nil {
		t.Fatalf("Submit() after freeing capacity failed: %v", err)
	}

	completed, err := service.Get(first.ID)
	if err != nil {
		t.Fatalf("Get(first) failed: %v", err)
	}

	wantCompleted := Job{
		ID:     first.ID,
		Text:   first.Text,
		Status: StatusCompleted,
		Result: Process(first.Text),
	}
	if completed != wantCompleted {
		t.Errorf("completed job = %+v, want %+v", completed, wantCompleted)
	}

	running, err := service.Get(second.ID)
	if err != nil {
		t.Fatalf("Get(second) failed: %v", err)
	}

	wantRunning := Job{
		ID:     second.ID,
		Text:   "document two",
		Status: StatusRunning,
	}
	if running != wantRunning {
		t.Errorf("running job = %+v, want %+v", running, wantRunning)
	}

	queued, err := service.Get(third.ID)
	if err != nil {
		t.Fatalf("Get(third) failed: %v", err)
	}

	wantQueued := Job{
		ID:     third.ID,
		Text:   "document three",
		Status: StatusQueued,
	}
	if queued != wantQueued {
		t.Errorf("queued job = %+v, want %+v", queued, wantQueued)
	}

	service.mu.Lock()
	storedCount := len(service.jobs)
	service.mu.Unlock()

	if storedCount != 3 || len(service.pending) != 1 {
		t.Errorf(
			"stored=%d, pending=%d, want 3 stored and 1 pending",
			storedCount,
			len(service.pending),
		)
	}
}

func TestService_ProcessingDoesNotHoldMutex(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	processor := func(ctx context.Context, text string) (Result, error) {
		if text == "document one" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return Result{}, ctx.Err()
			}
		}

		return Result{
			WordCount: 2,
			SHA256:    "test-digest",
		}, nil
	}

	service := newTestService(t, 1, 1, processor)

	first, err := service.Submit("document one")
	if err != nil {
		t.Fatalf("Submit() failed: %v", err)
	}

	accessDone := make(chan struct{})

	t.Cleanup(func() {
		close(release)
		service.Cancel()
		select {
		case <-accessDone:
		case <-time.After(5 * time.Second):
			t.Error("lookup/submission goroutine did not exit during cleanup")
		}
	})

	go func() {
		defer close(accessDone)

		select {
		case <-started:
		case <-service.done:
			t.Error("workers stopped without reaching the processor")
			return
		}

		running, err := service.Get(first.ID)
		if err != nil {
			t.Errorf("Get() during processing failed: %v", err)
		} else {
			want := Job{
				ID:     first.ID,
				Text:   first.Text,
				Status: StatusRunning,
			}

			if running != want {
				t.Errorf("running job = %+v, want %+v", running, want)
			}
		}

		if _, err := service.Submit("document two"); err != nil {
			t.Errorf("Submit() during processing failed: %v", err)
		}
	}()

	select {
	case <-accessDone:
	case <-time.After(5 * time.Second):
		t.Fatal("lookup and submission could not finish while processing was paused")
	}
}

func TestService_ConcurrentOperations(t *testing.T) {
	const count = 32

	service := newTestService(t, count, 4, ProcessWithContext)
	submitted := make([]Job, count)
	start := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(index int) {
			defer wg.Done()
			<-start

			text := fmt.Sprintf("document %d", index)
			job, err := service.Submit(text)
			if err != nil {
				t.Errorf("Submit(%q) failed: %v", text, err)
				return
			}
			submitted[index] = job

			snapshot, err := service.Get(job.ID)
			if err != nil {
				t.Errorf("Get(%q) failed: %v", job.ID, err)
			} else if snapshot.ID != job.ID || snapshot.Text != text {
				t.Errorf("unexpected snapshot for %q: %+v", text, snapshot)
			}

		}(i)
	}

	close(start)
	wg.Wait()

	service.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := service.Wait(ctx); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}

	seen := make(map[string]bool)

	for index, job := range submitted {
		if job.ID == "" {
			t.Errorf("submission %d has an empty job ID", index)
			continue
		}

		if seen[job.ID] {
			t.Errorf("duplicate job ID: %q", job.ID)
		}
		seen[job.ID] = true

		text := fmt.Sprintf("document %d", index)
		want := Job{
			ID:     job.ID,
			Text:   text,
			Status: StatusCompleted,
			Result: Process(text),
		}

		completed, err := service.Get(job.ID)
		if err != nil {
			t.Errorf("Get(%q) failed: %v", job.ID, err)
			continue
		}

		if completed != want {
			t.Errorf("completed job = %+v, want %+v", completed, want)
		}
	}

	if len(service.jobs) != count || len(service.pending) != 0 {
		t.Errorf(
			"stored=%d, pending=%d, want %d stored and 0 pending",
			len(service.jobs),
			len(service.pending),
			count,
		)
	}
}
