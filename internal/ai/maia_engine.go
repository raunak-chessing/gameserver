package ai

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type EvalRequest struct {
	FEN      string
	Response chan EvalResult
}

type EvalResult struct {
	BestMove string
	Err      error
}

// MaiaEngine wraps the lc0 (Leela Chess Zero) binary loaded with Maia weights
// It implements a Worker Pool to handle high concurrency without spawning OS processes per request.
type MaiaEngine struct {
	elo       int
	workers   []*maiaWorker
	jobQueue  chan EvalRequest
	waitGroup sync.WaitGroup
	cancel    context.CancelFunc
}

type maiaWorker struct {
	id     int
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	mu     sync.Mutex
}

// NewMaiaEngine initializes a new engine pool
func NewMaiaEngine(elo int, poolSize int) (*MaiaEngine, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	engine := &MaiaEngine{
		elo:      elo,
		jobQueue: make(chan EvalRequest, 1000), // buffered queue
		cancel:   cancel,
	}

	weightFile := fmt.Sprintf("weights/maia-%d.pb.gz", elo)

	for i := 0; i < poolSize; i++ {
		worker, err := createWorker(i, weightFile)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to start worker %d: %v", i, err)
		}
		engine.workers = append(engine.workers, worker)
		
		engine.waitGroup.Add(1)
		go engine.workerLoop(ctx, worker)
	}

	log.Printf("Initialized MaiaEngine %d ELO with %d workers\n", elo, poolSize)
	return engine, nil
}

func createWorker(id int, weightFile string) (*maiaWorker, error) {
	cmd := exec.Command("lc0", "--weights="+weightFile)
	
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	
	return &maiaWorker{
		id:     id,
		cmd:    cmd,
		stdin:  stdin,
		stdout: bufio.NewReader(stdout),
	}, nil
}

func (e *MaiaEngine) workerLoop(ctx context.Context, worker *maiaWorker) {
	defer e.waitGroup.Done()
	
	for {
		select {
		case <-ctx.Done():
			// Shutdown
			worker.mu.Lock()
			fmt.Fprintf(worker.stdin, "quit\n")
			worker.cmd.Wait()
			worker.mu.Unlock()
			return
		case req := <-e.jobQueue:
			move, err := worker.evaluate(req.FEN)
			req.Response <- EvalResult{BestMove: move, Err: err}
		}
	}
}

func (w *maiaWorker) evaluate(fen string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := fmt.Fprintf(w.stdin, "position fen %s\n", fen)
	if err != nil {
		return "", err
	}

	_, err = fmt.Fprintf(w.stdin, "go nodes 1\n")
	if err != nil {
		return "", err
	}

	for {
		line, err := w.stdout.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimSpace(line)
		
		if strings.HasPrefix(line, "bestmove") {
			parts := strings.Split(line, " ")
			if len(parts) >= 2 {
				return parts[1], nil
			}
		}
	}
}

// GetBestMove asks the worker pool to evaluate the position.
// It implements a 5-second timeout to prevent stalling the server.
func (e *MaiaEngine) GetBestMove(fen string) (string, error) {
	resChan := make(chan EvalResult, 1)
	
	select {
	case e.jobQueue <- EvalRequest{FEN: fen, Response: resChan}:
		// Successfully queued
	default:
		return "", fmt.Errorf("MaiaEngine %d queue is full (system overloaded)", e.elo)
	}

	select {
	case res := <-resChan:
		return res.BestMove, res.Err
	case <-time.After(5 * time.Second):
		return "", fmt.Errorf("MaiaEngine %d timeout evaluating position", e.elo)
	}
}

// Close gracefully terminates all workers in the pool
func (e *MaiaEngine) Close() {
	e.cancel()
	e.waitGroup.Wait()
}
