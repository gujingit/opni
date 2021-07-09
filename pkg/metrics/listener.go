package metrics

import "context"

type ListenerManager struct {
	activeListeners map[string]context.CancelFunc
}

func NewListenerManager() *ListenerManager {
	return &ListenerManager{
		activeListeners: make(map[string]context.CancelFunc),
	}
}

func (lm *ListenerManager) ListenerExists(name string) bool {
	_, ok := lm.activeListeners[name]
	return ok
}

func (lm *ListenerManager) AddListener(name string) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	lm.activeListeners[name] = cancel
	return ctx
}

func (lm *ListenerManager) RemoveListener(name string) {
	cancel, ok := lm.activeListeners[name]
	if ok {
		cancel()
	}
}
