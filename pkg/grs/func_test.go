package grs

import (
	"strings"
	"testing"
)

func TestTryPassesNonErrorPanicToRecovery(t *testing.T) {
	var recovered error
	Try(func() {
		panic("boom")
	}, func(err error) {
		recovered = err
	})

	if recovered == nil || !strings.Contains(recovered.Error(), "boom") {
		t.Fatalf("recovered error = %v, want panic value", recovered)
	}
}
