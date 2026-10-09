// Package check makes fixture setup and cleanup failures fail the child process.
package check

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
)

// Must rejects an unexpected fixture failure, including cleanup failures.
func Must(err error) {
	if err != nil {
		panic(err)
	}
}

// Value extracts a successful result without discarding its error.
func Value[T any](value T, err error) T { Must(err); return value }

// Served accepts the documented cancellation outcomes of a stopped listener.
func Served(err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
		return
	}
	Must(err)
}

// Executable identifies the running fixture, independent of argv[0].
func Executable() string { return Value(os.Executable()) }

// ReadFile confines fixture reads to the supplied directory.
func ReadFile(path string) (data []byte, retErr error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, root.Close()) }()
	return root.ReadFile(filepath.Base(path))
}

// WriteFile confines fixture writes to the supplied directory.
func WriteFile(path string, data []byte, mode os.FileMode) (retErr error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, root.Close()) }()
	return root.WriteFile(filepath.Base(path), data, mode)
}
