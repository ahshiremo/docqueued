package jobs

type Status string
type FailureKind string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

const (
	FailureProcessing FailureKind = "processing"
	FailureCancelled  FailureKind = "cancelled"
)

type Result struct {
	WordCount uint64
	SHA256    string
}

type Job struct {
	ID      string
	Text    string
	Status  Status
	Result  Result
	Failure FailureKind
}
