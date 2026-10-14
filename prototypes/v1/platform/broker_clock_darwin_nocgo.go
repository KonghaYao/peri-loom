//go:build darwin && !cgo

package platform

import "errors"

func continuousNanos() (uint64, error) {
	return 0, errors.New("Darwin broker requires CGO continuous clock")
}
