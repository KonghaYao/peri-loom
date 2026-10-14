package platform

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type p6VerifyLog struct {
	mutex sync.Mutex
	data  []byte
}

func (log *p6VerifyLog) Write(data []byte) (int, error) {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	log.data = append(log.data, data...)
	if len(log.data) > 65536 {
		log.data = log.data[len(log.data)-65536:]
	}
	return len(data), nil
}

func (log *p6VerifyLog) text(secrets []string) string {
	log.mutex.Lock()
	defer log.mutex.Unlock()
	text := string(log.data)
	for _, secret := range secrets {
		text = strings.ReplaceAll(text, secret, "[redacted]")
	}
	return text
}

type p6VerifyProcess struct {
	command  *exec.Cmd
	finished chan struct{}
	err      error
	log      p6VerifyLog
	ready    map[string]any
}

func p6VerifyEnvironment(extra map[string]string) []string {
	var environment []string
	for _, name := range []string{"PATH", "HOME", "TMPDIR", "LANG", "TZ"} {
		if value, exists := os.LookupEnv(name); exists {
			environment = append(environment, name+"="+value)
		}
	}
	for name, value := range extra {
		environment = append(environment, name+"="+value)
	}
	return environment
}

func p6VerifyToken(test *testing.T) string {
	test.Helper()
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		test.Fatal(err)
	}
	return hex.EncodeToString(bytes[:])
}

func p6VerifyStart(test *testing.T, binary string, arguments []string, environment map[string]string) *p6VerifyProcess {
	test.Helper()
	process := &p6VerifyProcess{command: exec.Command(binary, arguments...), finished: make(chan struct{})}
	process.command.Env = p6VerifyEnvironment(environment)
	process.command.Stderr = &process.log
	output, err := process.command.StdoutPipe()
	if err != nil {
		test.Fatal(err)
	}
	if err := process.command.Start(); err != nil {
		test.Fatal(err)
	}
	test.Cleanup(func() { process.stop(test, false) })
	ready := make(chan []byte, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		if scanner.Scan() {
			ready <- append([]byte(nil), scanner.Bytes()...)
		}
		_, _ = io.Copy(&process.log, output)
	}()
	go func() {
		process.err = process.command.Wait()
		close(process.finished)
	}()
	select {
	case line := <-ready:
		if json.Unmarshal(line, &process.ready) != nil || process.ready["ready"] != true {
			test.Fatal("invalid process readiness envelope")
		}
	case <-process.finished:
		test.Fatalf("process exited before readiness: %v", process.err)
	case <-time.After(15 * time.Second):
		test.Fatal("process readiness deadline exceeded")
	}
	return process
}

func (process *p6VerifyProcess) stop(test *testing.T, clean bool) {
	test.Helper()
	select {
	case <-process.finished:
		return
	default:
	}
	signal := os.Signal(os.Kill)
	if clean {
		signal = syscall.SIGTERM
	}
	_ = process.command.Process.Signal(signal)
	select {
	case <-process.finished:
		if clean && process.err != nil {
			test.Errorf("clean platform exit failed: %v", process.err)
		}
	case <-time.After(15 * time.Second):
		_ = process.command.Process.Kill()
		<-process.finished
		test.Error("process shutdown deadline exceeded; kill is not clean completion")
	}
}
