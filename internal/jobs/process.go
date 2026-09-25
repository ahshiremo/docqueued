package jobs

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func Process(text string) Result {
	words := strings.Fields(text)
	hash := sha256.Sum256([]byte(text))

	return Result{
		uint64(len(words)),
		hex.EncodeToString(hash[:]),
	}
}
