package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 0)
	defer cancelWait()
	checkWaitDoesNotCancelWorkers(t, waitCtx, context.DeadlineExceeded)
}

func checkWaitDoesNotCancelWorkers(t *testing.T, waitCtx context.Context, wantErr error) {
	t.Helper()

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

	if err := service.Wait(waitCtx); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() error = %v, want %v", err, wantErr)
	}

	running, err := service.Get(submitted.ID)
	if err != nil {
		t.Fatalf("Get() while processing failed:  %v", err)
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
	waitForServiceStop(t, service)
	assertStoredJob(t, service, Job{
		ID: submitted.ID, Text: text, Status: StatusCompleted, Result: Process(text),
	})
}

func TestService_WaitCancelledContext(t *testing.T) {
	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	checkWaitDoesNotCancelWorkers(t, waitCtx, context.Canceled)
}

func TestService_WaitNonCooperativeProcessor(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	releaseWorker := sync.OnceFunc(func() { close(release) })
	processor := func(_ context.Context, text string) (Result, error) {
		close(started)
		<-release
		return Process(text), nil
	}
	service := newRunningTestService(t, 1, 1, processor)
	t.Cleanup(releaseWorker)
	job := submitLifecycleJob(t, service, "noncooperative work")
	waitForTestSignal(t, started, "processor to start")

	service.Cancel()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 0)
	defer cancelWait()
	if err := service.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() = %v, want DeadlineExceeded", err)
	}
	assertStoredJob(t, service, Job{ID: job.ID, Text: job.Text, Status: StatusRunning})

	releaseWorker()
	waitForServiceStop(t, service)
	assertStoredJob(t, service, Job{
		ID: job.ID, Text: job.Text, Status: StatusCompleted, Result: Process(job.Text),
	})
}

func TestService_WorkerLimit(t *testing.T) {
	const workers = 3
	const jobs = 12
	started := make(chan struct{}, jobs)
	release := make(chan struct{})
	releaseWorkers := sync.OnceFunc(func() { close(release) })
	var active, peak, calls atomic.Int32
	processor := func(workerCtx context.Context, text string) (Result, error) {
		current := active.Add(1)
		defer active.Add(-1)
		calls.Add(1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			return ProcessWithContext(workerCtx, text)
		case <-workerCtx.Done():
			return Result{}, workerCtx.Err()
		}
	}
	service := newRunningTestService(t, jobs, workers, processor)
	t.Cleanup(releaseWorkers)
	submitted := make([]Job, jobs)
	for i := range submitted {
		submitted[i] = submitLifecycleJob(t, service, fmt.Sprintf("document %d", i))
	}
	for i := 0; i < workers; i++ {
		waitForTestSignal(t, started, "all configured workers to start")
	}
	if got := active.Load(); got != workers {
		t.Errorf("active workers = %d, want %d", got, workers)
	}

	service.BeginDrain()
	releaseWorkers()
	waitForServiceStop(t, service)
	if got := peak.Load(); got != workers {
		t.Errorf("peak workers = %d, want %d", got, workers)
	}
	if active.Load() != 0 || calls.Load() != jobs {
		t.Errorf("active=%d, calls=%d, want 0 active and %d calls", active.Load(), calls.Load(), jobs)
	}
	for _, job := range submitted {
		assertStoredJob(t, service, Job{
			ID: job.ID, Text: job.Text, Status: StatusCompleted, Result: Process(job.Text),
		})
	}
}

func TestService_DrainRejectsSubmissions(t *testing.T) {
	service := newRunningTestService(t, 1, 1, ProcessWithContext)
	service.BeginDrain()
	job, err := service.Submit("too late")
	if !errors.Is(err, ErrDraining) || job != (Job{}) {
		t.Errorf("Submit() = (%+v, %v), want empty job and ErrDraining", job, err)
	}
	waitForServiceStop(t, service)
	if len(service.jobs) != 0 || len(service.pending) != 0 {
		t.Error("rejected submission left stored or queued work")
	}
}

func TestService_DrainCompletesAcceptedJobs(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	releaseWorker := sync.OnceFunc(func() { close(release) })
	var inputs []string
	processor := func(workerCtx context.Context, text string) (Result, error) {
		inputs = append(inputs, text)
		if text == "first" {
			close(started)
			select {
			case <-release:
			case <-workerCtx.Done():
				return Result{}, workerCtx.Err()
			}
		}
		return ProcessWithContext(workerCtx, text)
	}
	service := newRunningTestService(t, 2, 1, processor)
	t.Cleanup(releaseWorker)
	first := submitLifecycleJob(t, service, "first")
	waitForTestSignal(t, started, "first job to start")
	second := submitLifecycleJob(t, service, "second")
	third := submitLifecycleJob(t, service, "third")
	service.BeginDrain()

	select {
	case <-service.done:
		t.Fatal("drain reported completion with active work")
	default:
	}
	releaseWorker()
	waitForServiceStop(t, service)
	for _, job := range []Job{first, second, third} {
		assertStoredJob(t, service, Job{
			ID: job.ID, Text: job.Text, Status: StatusCompleted, Result: Process(job.Text),
		})
	}
	if len(inputs) != 3 || inputs[0] != "first" || inputs[1] != "second" || inputs[2] != "third" {
		t.Errorf("processor inputs = %v, want first, second, third once each", inputs)
	}
}

func TestService_ShutdownRepeated(t *testing.T) {
	for _, mode := range []string{"drain", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			service := newRunningTestService(t, 1, 1, ProcessWithContext)
			for i := 0; i < 3; i++ {
				if mode == "drain" {
					service.BeginDrain()
				} else {
					service.Cancel()
				}
			}
			waitForServiceStop(t, service)
			for i := 0; i < 3; i++ {
				service.BeginDrain()
				service.Cancel()
				waitForServiceStop(t, service)
			}
		})
	}
}

func TestService_CancelActiveJob(t *testing.T) {
	started := make(chan struct{})
	processor := func(workerCtx context.Context, _ string) (Result, error) {
		close(started)
		<-workerCtx.Done()
		return Result{WordCount: 99, SHA256: "discard this partial result"}, workerCtx.Err()
	}
	service := newRunningTestService(t, 1, 1, processor)
	job := submitLifecycleJob(t, service, "active job")
	waitForTestSignal(t, started, "processor to start")
	service.Cancel()
	waitForServiceStop(t, service)
	assertStoredJob(t, service, Job{
		ID: job.ID, Text: job.Text, Status: StatusFailed, Failure: FailureCancelled,
	})
}

func TestService_CancelLeavesQueuedJobs(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int32
	processor := func(workerCtx context.Context, _ string) (Result, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-workerCtx.Done()
		return Result{}, workerCtx.Err()
	}
	service := newRunningTestService(t, 2, 1, processor)
	active := submitLifecycleJob(t, service, "active")
	waitForTestSignal(t, started, "processor to start")
	queued := []Job{
		submitLifecycleJob(t, service, "queued one"),
		submitLifecycleJob(t, service, "queued two"),
	}
	service.Cancel()
	waitForServiceStop(t, service)
	assertStoredJob(t, service, Job{
		ID: active.ID, Text: active.Text, Status: StatusFailed, Failure: FailureCancelled,
	})
	for _, job := range queued {
		assertStoredJob(t, service, job)
	}
	if calls.Load() != 1 || len(service.pending) != 2 || len(service.jobs) != 3 {
		t.Errorf("calls=%d, pending=%d, stored=%d, want 1, 2, 3",
			calls.Load(), len(service.pending), len(service.jobs))
	}
}

func TestService_CancelAfterDequeue(t *testing.T) {
	var calls int
	service, err := NewService(1, 1, func(context.Context, string) (Result, error) {
		calls++
		return Result{}, nil
	})
	if err != nil {
		t.Fatalf("NewService() failed: %v", err)
	}

	// Arrange the internal handoff after dequeue without racing a live worker.
	job := Job{ID: "dequeued", Text: "not started", Status: StatusQueued}
	service.jobs[job.ID] = job
	service.pending <- job.ID
	id := <-service.pending
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	cancelWorker()
	service.processJob(workerCtx, id)

	if calls != 0 {
		t.Errorf("processor calls = %d, want 0", calls)
	}
	assertStoredJob(t, service, Job{
		ID: job.ID, Text: job.Text, Status: StatusFailed, Failure: FailureCancelled,
	})
	if len(service.jobs) != 1 || len(service.pending) != 0 {
		t.Error("dequeued job was lost or requeued")
	}
}

func TestService_ShutdownBeforeStart(t *testing.T) {
	for _, mode := range []string{"drain", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			service, err := NewService(1, 1, ProcessWithContext)
			if err != nil {
				t.Fatalf("NewService() failed: %v", err)
			}
			cleanupLifecycleService(t, service)
			if mode == "drain" {
				service.BeginDrain()
			} else {
				service.Cancel()
			}
			service.BeginDrain()
			service.Cancel()
			select {
			case <-service.done:
			default:
				t.Error("shutdown before startup did not close done")
			}
			if err := service.Start(); !errors.Is(err, ErrDraining) {
				t.Errorf("Start() = %v, want ErrDraining", err)
			}
			job, err := service.Submit("too late")
			if !errors.Is(err, ErrDraining) || job != (Job{}) {
				t.Errorf("Submit() = (%+v, %v), want empty job and ErrDraining", job, err)
			}
			waitCtx, cancelWait := context.WithTimeout(context.Background(), 0)
			defer cancelWait()
			if err := service.Wait(waitCtx); !errors.Is(err, ErrNotStarted) {
				t.Errorf("Wait() = %v, want ErrNotStarted", err)
			}
			if len(service.jobs) != 0 || len(service.pending) != 0 {
				t.Error("shutdown-before-start accepted work")
			}
		})
	}
}

func TestService_ProcessingFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want FailureKind
	}{
		{"ordinary", errors.New("private document details"), FailureProcessing},
		{"cancelled", context.Canceled, FailureCancelled},
		{"deadline", context.DeadlineExceeded, FailureCancelled},
		{"wrapped cancellation", fmt.Errorf("private details: %w", context.Canceled), FailureCancelled},
		{"wrapped deadline", fmt.Errorf("private details: %w", context.DeadlineExceeded), FailureCancelled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor := func(context.Context, string) (Result, error) {
				return Result{WordCount: 99, SHA256: "partial"}, tt.err
			}
			service := newRunningTestService(t, 1, 1, processor)
			job := submitLifecycleJob(t, service, "document")
			service.BeginDrain()
			waitForServiceStop(t, service)
			assertStoredJob(t, service, Job{
				ID: job.ID, Text: job.Text, Status: StatusFailed, Failure: tt.want,
			})
		})
	}
}

func TestService_CompletionWinsCancellation(t *testing.T) {
	started := make(chan struct{})
	processor := func(workerCtx context.Context, text string) (Result, error) {
		close(started)
		<-workerCtx.Done()
		return Process(text), nil
	}
	service := newRunningTestService(t, 1, 1, processor)
	job := submitLifecycleJob(t, service, "successful result")
	waitForTestSignal(t, started, "processor to start")
	service.Cancel()
	waitForServiceStop(t, service)
	assertStoredJob(t, service, Job{
		ID: job.ID, Text: job.Text, Status: StatusCompleted, Result: Process(job.Text),
	})
}

func TestService_ConcurrentStart(t *testing.T) {
	const callers = 16
	service, err := NewService(1, 2, ProcessWithContext)
	if err != nil {
		t.Fatalf("NewService() failed: %v", err)
	}
	cleanupLifecycleService(t, service)
	start := make(chan struct{})
	results := make(chan error, callers)
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer callersDone.Done()
			<-start
			results <- service.Start()
		}()
	}
	close(start)
	callersDone.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrAlreadyStarted) {
			t.Errorf("Start() = %v, want ErrAlreadyStarted", err)
		}
	}
	if successes != 1 {
		t.Errorf("successful starts = %d, want 1", successes)
	}
	job := submitLifecycleJob(t, service, "work after racing starts")
	service.BeginDrain()
	waitForServiceStop(t, service)
	assertStoredJob(t, service, Job{
		ID: job.ID, Text: job.Text, Status: StatusCompleted, Result: Process(job.Text),
	})
}

func TestService_ConcurrentShutdown(t *testing.T) {
	for _, initiallyStarted := range []bool{false, true} {
		t.Run(fmt.Sprintf("started=%t", initiallyStarted), func(t *testing.T) {
			service, err := NewService(1, 2, ProcessWithContext)
			if err != nil {
				t.Fatalf("NewService() failed: %v", err)
			}
			cleanupLifecycleService(t, service)
			if initiallyStarted {
				if err := service.Start(); err != nil {
					t.Fatalf("Start() failed: %v", err)
				}
			}
			start := make(chan struct{})
			var callersDone sync.WaitGroup
			var successes atomic.Int32
			for i := 0; i < 18; i++ {
				callersDone.Add(1)
				go func() {
					defer callersDone.Done()
					<-start
					switch i % 3 {
					case 0:
						err := service.Start()
						if err == nil {
							successes.Add(1)
						} else if !errors.Is(err, ErrAlreadyStarted) && !errors.Is(err, ErrDraining) {
							t.Errorf("unexpected Start() error: %v", err)
						}
					case 1:
						service.BeginDrain()
					case 2:
						service.Cancel()
					}
				}()
			}
			close(start)
			callersDone.Wait()
			if successes.Load() > 1 || (initiallyStarted && successes.Load() != 0) {
				t.Errorf("unexpected additional successful starts: %d", successes.Load())
			}
			waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelWait()
			err = service.Wait(waitCtx)
			service.mu.Lock()
			started := service.started
			service.mu.Unlock()
			if started && err != nil {
				t.Fatalf("Wait() failed: %v", err)
			}
			if !started && !errors.Is(err, ErrNotStarted) {
				t.Fatalf("Wait() = %v, want ErrNotStarted", err)
			}
			waitForTestSignal(t, service.done, "shutdown completion")
			if err := service.Start(); !errors.Is(err, ErrAlreadyStarted) && !errors.Is(err, ErrDraining) {
				t.Errorf("restart after shutdown = %v, want lifecycle error", err)
			}
		})
	}
}

func TestService_SubmitDuringShutdown(t *testing.T) {
	const count = 32
	for _, mode := range []string{"drain", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			service := newRunningTestService(t, count, 4, ProcessWithContext)
			start := make(chan struct{})
			accepted := make(chan Job, count)
			var callersDone sync.WaitGroup
			for i := 0; i < count; i++ {
				callersDone.Add(1)
				go func() {
					defer callersDone.Done()
					<-start
					job, err := service.Submit(fmt.Sprintf("document %d", i))
					if err == nil {
						accepted <- job
					} else if !errors.Is(err, ErrDraining) || job != (Job{}) {
						t.Errorf("unexpected rejection: job=%+v, error=%v", job, err)
					}
				}()
			}
			callersDone.Add(1)
			go func() {
				defer callersDone.Done()
				<-start
				if mode == "drain" {
					service.BeginDrain()
				} else {
					service.Cancel()
				}
			}()
			close(start)
			callersDone.Wait()
			close(accepted)
			waitForServiceStop(t, service)

			seen := make(map[string]bool)
			for job := range accepted {
				if job.ID == "" || seen[job.ID] {
					t.Errorf("empty or duplicate accepted ID: %q", job.ID)
				}
				seen[job.ID] = true
				stored, err := service.Get(job.ID)
				if err != nil {
					t.Fatalf("accepted job missing: %v", err)
				}
				want := Job{ID: job.ID, Text: job.Text, Status: stored.Status}
				switch stored.Status {
				case StatusCompleted:
					want.Result = Process(job.Text)
				case StatusFailed:
					if mode != "cancel" {
						t.Error("graceful drain failed an accepted job")
					}
					want.Failure = FailureCancelled
				case StatusQueued:
					if mode != "cancel" {
						t.Error("graceful drain left an accepted job queued")
					}
				default:
					t.Errorf("unexpected state after shutdown: %q", stored.Status)
				}
				if stored != want {
					t.Errorf("stored job = %+v, want %+v", stored, want)
				}
			}
			if len(service.jobs) != len(seen) {
				t.Errorf("stored=%d, accepted=%d; possible orphan", len(service.jobs), len(seen))
			}
			pending := make(map[string]bool)
			for id := range service.pending {
				if !seen[id] || pending[id] || service.jobs[id].Status != StatusQueued {
					t.Errorf("unexpected queued ID: %q", id)
				}
				pending[id] = true
			}
			for id, job := range service.jobs {
				if (job.Status == StatusQueued) != pending[id] {
					t.Errorf("queue membership disagrees with status for %q", id)
				}
			}
			if job, err := service.Submit("after shutdown"); !errors.Is(err, ErrDraining) || job != (Job{}) {
				t.Errorf("post-shutdown Submit() = (%+v, %v)", job, err)
			}
		})
	}
}

func submitLifecycleJob(t *testing.T, service *Service, text string) Job {
	t.Helper()
	job, err := service.Submit(text)
	if err != nil {
		t.Fatalf("Submit(%q) failed: %v", text, err)
	}
	return job
}

func waitForServiceStop(t *testing.T, service *Service) {
	t.Helper()
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()
	if err := service.Wait(waitCtx); err != nil {
		t.Fatalf("Wait() failed: %v", err)
	}
}

func assertStoredJob(t *testing.T, service *Service, want Job) {
	t.Helper()
	got, err := service.Get(want.ID)
	if err != nil {
		t.Fatalf("Get(%q) failed: %v", want.ID, err)
	}
	if got != want {
		t.Errorf("stored job = %+v, want %+v", got, want)
	}
}

func cleanupLifecycleService(t *testing.T, service *Service) {
	t.Helper()
	t.Cleanup(func() {
		service.Cancel()
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelWait()
		if err := service.Wait(waitCtx); err != nil && !errors.Is(err, ErrNotStarted) {
			t.Errorf("service cleanup failed: %v", err)
		}
	})
}
