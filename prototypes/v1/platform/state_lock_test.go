package platform

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestStateLockChild(test *testing.T) {
	directory := os.Getenv("PERI_P6_STATE_LOCK_DIR")
	if directory == "" {
		test.Skip("subprocess helper")
	}
	if store, err := OpenStateStore(directory); !errors.Is(err, ErrStateLocked) {
		if store != nil {
			store.Close()
		}
		test.Fatalf("cross-process lock missing: %v", err)
	}
}
func TestStateActualCrossProcessLock(test *testing.T) {
	directory := test.TempDir()
	store, err := OpenStateStore(directory)
	if err != nil {
		test.Fatal(err)
	}
	defer store.Close()
	command := exec.Command(os.Args[0], "-test.run=^TestStateLockChild$")
	command.Env = []string{"PERI_P6_STATE_LOCK_DIR=" + directory}
	if output, err := command.CombinedOutput(); err != nil {
		test.Fatalf("lock subprocess: %s %v", output, err)
	}
}
