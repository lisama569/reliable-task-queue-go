package queue

import "sync"

// workerGroup is a minimal wait group wrapper.
type workerGroup struct {
	wg sync.WaitGroup
}

func (g *workerGroup) add(fn func()) {
	g.wg.Add(1)
	go func() { defer g.wg.Done(); fn() }()
}

func (g *workerGroup) wait() { g.wg.Wait() }
