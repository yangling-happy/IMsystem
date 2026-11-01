package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"imsystem/internal/model"
	"imsystem/internal/storage"
)

// fakeRepo 记录调用次数，并可按顺序注入错误（nil 表示成功）。
type fakeRepo struct {
	mu     sync.Mutex
	calls  int
	errors []error
}

func (f *fakeRepo) SaveMessage(ctx context.Context, m model.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= len(f.errors) {
		return f.errors[f.calls-1]
	}
	return nil
}

func (f *fakeRepo) Close() error { return nil }

func (f *fakeRepo) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newTestWorker(repo storage.Repo) *Worker {
	w := NewWorker(repo)
	w.backoff = time.Millisecond // 测试里不需要真的等
	w.popWait = 50 * time.Millisecond
	return w
}

func testMessage() model.Message {
	return model.Message{
		MsgID:    "m-1",
		From:     "alice",
		To:       "bob",
		ChatType: model.ChatSingle,
		Body:     "hello",
		SentAt:   time.Now(),
	}
}

func TestPersistSavesOnce(t *testing.T) {
	repo := &fakeRepo{}

	newTestWorker(repo).persist(context.Background(), testMessage())

	if got := repo.callCount(); got != 1 {
		t.Fatalf("落库调用次数 got %d, want 1", got)
	}
}

func TestPersistStopsOnDuplicate(t *testing.T) {
	repo := &fakeRepo{errors: []error{storage.ErrDuplicate}}

	newTestWorker(repo).persist(context.Background(), testMessage())

	if got := repo.callCount(); got != 1 {
		t.Fatalf("重复消息不该重试，调用次数 got %d, want 1", got)
	}
}

func TestPersistRetriesThenSucceeds(t *testing.T) {
	errTemp := errors.New("db: connection reset")
	repo := &fakeRepo{errors: []error{errTemp, errTemp}}

	newTestWorker(repo).persist(context.Background(), testMessage())

	if got := repo.callCount(); got != 3 {
		t.Fatalf("两次失败后应第三次成功，调用次数 got %d, want 3", got)
	}
}

func TestPersistGivesUpAfterMaxRetry(t *testing.T) {
	errTemp := errors.New("db: unavailable")
	repo := &fakeRepo{errors: []error{errTemp, errTemp, errTemp, errTemp}}

	w := newTestWorker(repo)
	w.persist(context.Background(), testMessage())

	// 1 次初始尝试 + 3 次重试 = 4
	want := 1 + w.maxRetry
	if got := repo.callCount(); got != want {
		t.Fatalf("重试耗尽的调用次数 got %d, want %d", got, want)
	}
}

func TestWorkerConsumesFromQueue(t *testing.T) {
	repo := &fakeRepo{}
	q := NewMemoryQueue(2)
	ctx := context.Background()

	if err := q.Push(ctx, testMessage()); err != nil {
		t.Fatalf("入队失败: %v", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		newTestWorker(repo).Run(runCtx, q)
		close(done)
	}()

	// 轮询等待落库完成，避免用固定 sleep 导致测试不稳定
	deadline := time.After(time.Second)
	for repo.callCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("Worker 没有消费到队列里的消息")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ctx 取消后 Run 没有退出")
	}
}

func TestMemoryQueuePushWhenFull(t *testing.T) {
	q := NewMemoryQueue(1)
	ctx := context.Background()

	if err := q.Push(ctx, testMessage()); err != nil {
		t.Fatalf("第一次入队不该失败: %v", err)
	}
	if err := q.Push(ctx, testMessage()); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("队列满时应返回 ErrQueueFull, got %v", err)
	}
}
