package main

import (
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestNodeStartAndStop verifies that main() starts the node and logs “Node started successfully”,
// then we send a SIGTERM which triggers graceful shutdown (logs “Node stopped.”) and “Received shutdown signal”.
func TestNodeStartAndStop(t *testing.T) {
	// 1) Save old os.Args and old stderr
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	oldStderr := os.Stderr
	defer func() { os.Stderr = oldStderr }()

	// 2) Provide test flags
	os.Args = []string{
		"node",
		"--p2pPort=999",
		"--enableTor=true",
		"--dbPath=/tmp/6529db-test",
	}

	// 3) Capture stderr in a pipe, because production zap logs go to stderr by default
	r, w, _ := os.Pipe()
	os.Stderr = w

	// 4) Run main() in a separate goroutine
	done := make(chan struct{})
	go func() {
		main()
		close(done) // signals that main() has exited
	}()

	// 5) Give main() enough time to print “Node started successfully”
	time.Sleep(300 * time.Millisecond)

	// 6) Send SIGTERM to ourselves => triggers main() signal handler
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatalf("failed to find our own process: %v", err)
	}
	_ = proc.Signal(syscall.SIGTERM)

	// 7) Wait for main() to exit
	select {
	case <-done:
		// Good, main() exited
	case <-time.After(2 * time.Second):
		t.Fatal("main() did not exit after sending SIGTERM")
	}

	// 8) Now that main() is done, close the writer side of the pipe
	w.Close()

	// 9) Read whatever was written to stderr
	outBytes, _ := io.ReadAll(r)
	out := string(outBytes)

	// 10) Check logs
	if !strings.Contains(out, "Node started successfully") {
		t.Errorf("Expected 'Node started successfully', but not found. Output:\n%s", out)
	}
	if !strings.Contains(out, "Node stopped.") {
		t.Errorf("Expected 'Node stopped.', but not found. Output:\n%s", out)
	}
	if !strings.Contains(out, "Received shutdown signal") {
		t.Errorf("Expected 'Received shutdown signal', but not found. Output:\n%s", out)
	}
}
