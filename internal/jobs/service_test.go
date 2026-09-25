package jobs

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestService(t *testing.T, capacity int) *Service {
	t.Helper()

	service, err := NewService(capacity, Process)
	if err != nil {
		t.Fatalf("NewService() failed: %v", err)
	}
	return service
}

func TestNewService(t *testing.T) {
	tests := []struct {
		name      string
		capacity  int
		processor func(string) Result
		wantErr   bool
	}{
		{
			"valid configuration",
			2,
			Process,
			false,
		},
		{
			"zero capacity",
			0,
			Process,
			true,
		},
		{
			"negative capacity",
			-1,
			Process,
			true,
		},
		{
			"nil processor",
			2,
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service, err := NewService(tt.capacity, tt.processor)
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

			if service.ProcessNext() {
				t.Error("new service unexpectedly had pending work")
			}
		})
	}
}

func TestService_Get(t *testing.T) {
	service := newTestService(t, 1)

	submitted, err := service.Submit("hello world")
	if err != nil {
		t.Fatalf("Submit() failed %v", err)
	}

	snapshot, err := service.Get(submitted.ID)
	if err != nil {
		t.Fatalf("Get() failed %v", err)
	}

	if snapshot != submitted {
		t.Fatalf("Get() = %+v,  want %+v", snapshot, submitted)
	}

	snapshot.Text = "change by caller"
	snapshot.Status = StatusCompleted
	snapshot.Result = Result{
		WordCount: 999,
		SHA256:    "changed",
	}

	stored, err := service.Get(submitted.ID)
	if err != nil {
		t.Fatalf("second Get() failed %v", err)
	}

	if stored != submitted {
		t.Errorf("stored job changed: got %+v,  want %+v", stored, submitted)
	}
}

func TestService_GetNotFound(t *testing.T) {
	service := newTestService(t, 1)
	job, err := service.Get("not found")

	if !errors.Is(err, ErrNotFound) {
		t.Errorf("error = %v, want ErrNotFound", err)
	}

	if job != (Job{}) {
		t.Errorf("expected an empty job, recieved %v", job)
	}
}

func TestService_Submit(t *testing.T) {
	service := newTestService(t, 1)
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
			service := newTestService(t, 1)
			job, err := service.Submit(tt.text)

			if !errors.Is(err, ErrBlankText) {
				t.Errorf("error = %v, want ErrBlankText", err)
			}

			if job != (Job{}) {
				t.Errorf("job = %v, expected empty", job)
			}

			if len(service.jobs) != 0 || len(service.pending) != 0 {
				t.Errorf("jobs length = %d, pending length = %d, expected 0", len(service.jobs), len(service.pending))
			}
		})
	}
}

func TestService_SubmitQueueFull(t *testing.T) {
	service := newTestService(t, 1)

	first, err := service.Submit("document one")
	if err != nil {
		t.Fatalf("first Submit() failed: %v", err)
	}

	rejected, err := service.Submit("document two")

	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("error = %v, want ErrQueueFull", err)
	}

	if rejected != (Job{}) {
		t.Errorf("rejected job = %+v, want an empty Job", rejected)
	}

	if len(service.jobs) != 1 || len(service.pending) != 1 {
		t.Errorf(
			"stored=%d, pending=%d, want 1 each",
			len(service.jobs),
			len(service.pending),
		)
	}

	stored, err := service.Get(first.ID)
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}

	if stored != first {
		t.Errorf("accepted job changed: got %+v, want %+v", stored, first)
	}
}

func TestService_ProcessNext(t *testing.T) {
	text := "hello\tworld!"
	result := Result{
		WordCount: 2,
		SHA256:    "test-digest",
	}

	var received string
	var calls int

	processor := func(input string) Result {
		received = input
		calls++
		return result
	}

	service, err := NewService(1, processor)
	if err != nil {
		t.Fatalf("NewService() failed: %v", err)
	}

	submitted, err := service.Submit(text)
	if err != nil {
		t.Fatalf("Submit() failed: %v", err)
	}

	if !service.ProcessNext() {
		t.Fatalf("expected job %q to be processed", submitted.ID)
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

	if service.ProcessNext() {
		t.Error("drained queue unexpectedly reported processing work")
	}

	if calls != 1 {
		t.Errorf("processor calls = %d, want 1", calls)
	}
}

func TestService_QueueCapacityReused(t *testing.T) {
	service := newTestService(t, 1)
	first, err := service.Submit("document one")
	if err != nil {
		t.Fatalf("first Submit() failed: %v", err)
	}

	if !service.ProcessNext() {
		t.Fatal("expected the first job to be processed")
	}

	second, err := service.Submit("document two")
	if err != nil {
		t.Fatalf("Submit() after processing failed: %v", err)
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

	queued, err := service.Get(second.ID)
	if err != nil {
		t.Fatalf("Get(second) failed: %v", err)
	}

	wantQueued := Job{
		ID:     second.ID,
		Text:   "document two",
		Status: StatusQueued,
	}
	if queued != wantQueued {
		t.Errorf("queued job = %+v, want %+v", queued, wantQueued)
	}

	if len(service.jobs) != 2 || len(service.pending) != 1 {
		t.Errorf(
			"stored=%d, pending=%d, want 2 stored and 1 pending",
			len(service.jobs),
			len(service.pending),
		)
	}
}

func TestService_ProcessingDoesNotHoldMutex(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	processor := func(_ string) Result {
		close(started)
		<-release

		return Result{
			WordCount: 2,
			SHA256:    "test-digest",
		}
	}

	service, err := NewService(1, processor)
	if err != nil {
		t.Fatalf("NewService() failed: %v", err)
	}
	if service == nil {
		t.Fatal("expected a service")
	}

	first, err := service.Submit("document one")
	if err != nil {
		t.Fatalf("Submit() failed: %v", err)
	}

	processingDone := make(chan struct{})
	accessDone := make(chan struct{})

	t.Cleanup(func() {
		close(release)
		<-processingDone
		<-accessDone
	})

	go func() {
		defer close(processingDone)

		if !service.ProcessNext() {
			t.Errorf("expected job %q to be processed", first.ID)
		}
	}()

	go func() {
		defer close(accessDone)

		select {
		case <-started:
		case <-processingDone:
			t.Error("processing finished without reaching the processor")
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

	service := newTestService(t, count)
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

			if !service.ProcessNext() {
				t.Errorf("expected pending work after submitting %q", text)
			}
		}(i)
	}

	close(start)
	wg.Wait()

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
