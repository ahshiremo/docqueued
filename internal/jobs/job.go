package jobs

type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
)

type Result struct {
	WordCount uint64
	SHA256    string
}

type Job struct {
	ID     string
	Text   string
	Status Status
	Result Result
}
