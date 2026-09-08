// Package metrics records per-command observations and aggregates them into
// interval snapshots and cumulative histograms.
package metrics

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/redis/go-redis/v9"
)

// ErrorClass buckets errors for reporting.
type ErrorClass string

const (
	ErrTimeout    ErrorClass = "timeout"
	ErrConnection ErrorClass = "connection"
	ErrCluster    ErrorClass = "cluster"
	ErrOOM        ErrorClass = "oom"
	ErrOther      ErrorClass = "other"
)

// Classify maps an error to an ErrorClass. nil maps to "".
func Classify(err error) ErrorClass {
	if err == nil {
		return ""
	}
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, redis.ErrPoolTimeout) ||
		(errors.As(err, &ne) && ne.Timeout()) {
		return ErrTimeout
	}
	var opErr *net.OpError
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.As(err, &opErr) {
		return ErrConnection
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "MOVED"), strings.HasPrefix(msg, "ASK"),
		strings.HasPrefix(msg, "CLUSTERDOWN"), strings.HasPrefix(msg, "TRYAGAIN"),
		strings.Contains(msg, "cluster down"):
		return ErrCluster
	case strings.HasPrefix(msg, "OOM"):
		return ErrOOM
	case strings.Contains(msg, "i/o timeout"):
		return ErrTimeout
	case strings.Contains(msg, "connection refused"), strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "broken pipe"):
		return ErrConnection
	}
	return ErrOther
}
