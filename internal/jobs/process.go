package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type Processor func(context.Context, string) (Result, error)

func Process(text string) Result {
	words := strings.Fields(text)
	hash := sha256.Sum256([]byte(text))

	return Result{
		uint64(len(words)),
		hex.EncodeToString(hash[:]),
	}
}

func ProcessWithContext(ctx context.Context, text string) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return Process(text), nil
}
