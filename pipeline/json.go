package pipeline

import (
	"encoding/json"
	"fmt"

	"github.com/justinclev/GoGuardLib/retry"
)

// SetJSON saves v as JSON under key for later steps, like Exec.Set. A value that cannot be
// marshalled can never succeed on a retry, so the error is marked retry.Permanent: return it from
// the step as it is.
func SetJSON(x *Exec, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return retry.Permanent(fmt.Errorf("pipeline: saving %q: %w", key, err))
	}
	x.Set(key, b)
	return nil
}

// GetJSON reads what SetJSON saved under key. ok is false when nothing was saved under key (a
// zero T is returned). Saved data that does not decode into T is a permanent error: the message was
// saved by a different version of the code, and retrying cannot help.
func GetJSON[T any](x *Exec, key string) (v T, ok bool, err error) {
	raw, ok := x.Get(key)
	if !ok {
		return v, false, nil
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		var zero T
		return zero, false, retry.Permanent(fmt.Errorf("pipeline: reading %q: %w", key, err))
	}
	return v, true, nil
}

// RequireJSON is GetJSON for data a step cannot do without: a missing key is a permanent error
// too (the checkpoint was written by code that did not save it), so the step can return the error
// as it is.
func RequireJSON[T any](x *Exec, key string) (T, error) {
	v, ok, err := GetJSON[T](x, key)
	if err == nil && !ok {
		err = retry.Permanent(fmt.Errorf("pipeline: %q was not saved by an earlier step", key))
	}
	return v, err
}
