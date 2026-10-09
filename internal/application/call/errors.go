// Package call maps application failures to public call errors.
package call

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Liapoldus/domain/internal/domain/interfaces"
	"github.com/Liapoldus/domain/internal/domain/models"
	"regexp"
)

// Product error codes, matching the domain v1 envelope codes.
const (
	CodeInvalidRequest = "invalid_request"
	CodeForbidden      = "forbidden"
	CodeNotFound       = "not_found"
	CodeConflict       = "conflict"
	CodeNotLeader      = "not_leader"
	CodeUnavailable    = "unavailable"
	CodeUnknownOutcome = "unknown_outcome"
	CodeQueryRejected  = "query_rejected"
	CodeInternal       = "internal"
	CodeEpochMismatch  = "epoch_mismatch"
)

func Marshal(data any) (json.RawMessage, *models.ProductError) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return nil, Failure(CodeInternal, false, false, "response serialization failed")
	}
	return encoded, nil
}

func Failure(code string, retryable, unknownOutcome bool, message string) *models.ProductError {
	return &models.ProductError{Code: code, Retryable: retryable, UnknownOutcome: unknownOutcome, Message: message}
}

// FromPort maps one interfaces sentinel to a business error. The
// not_leader hint is composed by the service before proposing/reading.
func FromPort(err error) *models.ProductError {
	switch {
	case errors.Is(err, interfaces.ErrForbidden):
		return Failure(CodeForbidden, false, false, "the caller scope is not permitted to perform this operation")
	case errors.Is(err, interfaces.ErrNotFound):
		return Failure(CodeNotFound, false, false, "the requested row does not exist")
	case errors.Is(err, interfaces.ErrConflict):
		return Failure(CodeConflict, false, false, "the write conflicts with existing data")
	case errors.Is(err, interfaces.ErrInvalid):
		return Failure(CodeInvalidRequest, false, false, "the request does not match the configured domain model")
	case errors.Is(err, interfaces.ErrNotLeader):
		return Failure(CodeNotLeader, true, false, "this domain node is not the raft leader")
	case errors.Is(err, interfaces.ErrUnavailable):
		return Failure(CodeUnavailable, true, false, "the domain cluster could not confirm the operation")
	case errors.Is(err, interfaces.ErrUnknownOutcome):
		return Failure(CodeUnknownOutcome, true, true, "the domain write outcome is unknown; retry with the same writeId")
	case errors.Is(err, interfaces.ErrEpochMismatch):
		return Failure(CodeEpochMismatch, true, false, "the client epoch does not match the current domain state")
	case errors.Is(err, context.DeadlineExceeded):
		return Failure(CodeUnknownOutcome, true, true, "the domain write outcome is unknown; retry with the same writeId")
	default:
		return Failure(CodeInternal, false, false, "an internal domain error occurred")
	}
}

var writeIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

func ValidWriteID(value string) bool { return writeIDPattern.MatchString(value) }
