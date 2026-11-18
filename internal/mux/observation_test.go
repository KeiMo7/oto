// Copyright 2026 The Oto Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mux_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ebitengine/oto/v3/internal/mux"
)

func TestObservationProgress(t *testing.T) {
	const sourceSize = 48000

	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	var mu sync.Mutex
	var samples []mux.Progress
	p, err := m.NewPlayerWithOptions(bytes.NewReader(make([]byte, sourceSize)), &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{
			ProgressInterval: 5 * time.Millisecond,
			OnProgress: func(progress mux.Progress) {
				mu.Lock()
				samples = append(samples, progress)
				mu.Unlock()
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBufferSize(512)
	p.Play()

	buf := make([]float32, 64)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.ReadFloat32s(buf)
		mu.Lock()
		increasing := 0
		last := int64(-1)
		for _, sample := range samples {
			if sample.Position > last {
				increasing++
				last = sample.Position
			}
		}
		mu.Unlock()
		if increasing >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = p.Close()

	mu.Lock()
	defer mu.Unlock()
	increasing := make([]mux.Progress, 0, len(samples))
	last := int64(-1)
	for _, sample := range samples {
		if sample.Position > last {
			increasing = append(increasing, sample)
			last = sample.Position
		}
	}
	if len(increasing) < 3 {
		t.Fatalf("got %d increasing progress callbacks; want at least 3: %+v", len(increasing), samples)
	}
	t.Logf("increasing progress callbacks=%d first=%d last=%d total=%d",
		len(increasing), increasing[0].Position, increasing[len(increasing)-1].Position, increasing[0].Total)
	for _, sample := range increasing {
		if sample.PlayerID == 0 {
			t.Errorf("progress has no player ID: %+v", sample)
		}
		if sample.Total != sourceSize {
			t.Errorf("progress total = %d; want %d", sample.Total, sourceSize)
		}
		if sample.TotalFrames != sourceSize {
			t.Errorf("progress total frames = %d; want %d", sample.TotalFrames, sourceSize)
		}
		if ratio, ok := sample.Ratio(); !ok || ratio > 1 {
			t.Errorf("Ratio() = %v, %v; want a ratio in [0, 1]", ratio, ok)
		}
	}
}

type observationStreamReader struct {
	remaining int
}

func (r *observationStreamReader) Read(buf []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(buf), r.remaining)
	clear(buf[:n])
	r.remaining -= n
	return n, nil
}

func TestObservationProgressWithUnknownTotal(t *testing.T) {
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	progressCh := make(chan mux.Progress, 16)
	p, err := m.NewPlayerWithOptions(&observationStreamReader{remaining: 1 << 20}, &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{
			ProgressInterval: 5 * time.Millisecond,
			OnProgress: func(progress mux.Progress) {
				select {
				case progressCh <- progress:
				default:
				}
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBufferSize(512)
	p.Play()

	buf := make([]float32, 64)
	deadline := time.After(2 * time.Second)
	for {
		m.ReadFloat32s(buf)
		select {
		case progress := <-progressCh:
			if progress.Position == 0 {
				continue
			}
			if progress.Total != -1 {
				t.Fatalf("progress total = %d; want -1 for a streaming source", progress.Total)
			}
			if progress.TotalFrames != -1 {
				t.Fatalf("progress total frames = %d; want -1 for a streaming source", progress.TotalFrames)
			}
			if progress.Position <= 0 {
				t.Fatalf("progress position = %d; want a positive position", progress.Position)
			}
			if _, ok := progress.Ratio(); ok {
				t.Fatal("Ratio reported a total for an unknown total")
			}
			_ = p.Close()
			return
		case <-deadline:
			t.Fatal("timed out waiting for progress")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestObservationCallbackPanicIsContained(t *testing.T) {
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	var calls atomic.Int64
	var logs lockedBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p, err := m.NewPlayerWithOptions(bytes.NewReader(make([]byte, 48000)), &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{
			ProgressInterval: time.Millisecond,
			OnProgress: func(mux.Progress) {
				if calls.Add(1) == 1 {
					panic("callback panic")
				}
			},
			Logger: logger,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBufferSize(512)
	p.Play()

	buf := make([]float32, 64)
	deadline := time.Now().Add(2 * time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		m.ReadFloat32s(buf)
		time.Sleep(5 * time.Millisecond)
	}
	_ = p.Close()
	if got := calls.Load(); got < 3 {
		t.Fatalf("progress callback was called %d times after a panic; want at least 3", got)
	}
	if !strings.Contains(logs.String(), "observation callback panicked") {
		t.Errorf("callback panic was not logged:\n%s", logs.String())
	}
}

func TestObservationSlowCallbackDoesNotBlockMixing(t *testing.T) {
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	p, err := m.NewPlayerWithOptions(bytes.NewReader(make([]byte, 48000)), &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{
			ProgressInterval: time.Millisecond,
			OnProgress: func(mux.Progress) {
				once.Do(func() {
					close(entered)
					<-release
				})
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBufferSize(512)
	p.Play()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("progress callback did not start")
	}

	start := time.Now()
	m.ReadFloat32s(make([]float32, 64))
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("ReadFloat32s took %s while a callback was blocked", elapsed)
	}
	close(release)
	_ = p.Close()
}

type observationSizedErrorReader struct {
	data []byte
	err  error
}

func (r *observationSizedErrorReader) Read(buf []byte) (int, error) {
	n := copy(buf, r.data)
	r.data = r.data[n:]
	return n, r.err
}

func (r *observationSizedErrorReader) Size() int64 {
	return int64(cap(r.data))
}

func TestObservationStructuredError(t *testing.T) {
	const dataSize = 256
	errSource := errors.New("read /home/alice/private/audio.pcm?token=top-secret failed")
	src := &observationSizedErrorReader{
		data: make([]byte, dataSize),
		err:  errSource,
	}

	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	errorCh := make(chan *mux.PlayerError, 1)
	p, err := m.NewPlayerWithOptions(src, &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{
			OnError: func(eventError *mux.PlayerError) {
				select {
				case errorCh <- eventError:
				default:
				}
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Play()

	var eventError *mux.PlayerError
	deadline := time.Now().Add(2 * time.Second)
	for eventError == nil && time.Now().Before(deadline) {
		m.ReadFloat32s(make([]float32, 64))
		select {
		case eventError = <-errorCh:
		default:
		}
		time.Sleep(time.Millisecond)
	}
	if eventError == nil {
		t.Fatal("timed out waiting for a structured error")
	}
	if eventError.Kind != mux.ErrorKindSourceRead {
		t.Errorf("error kind = %q; want %q", eventError.Kind, mux.ErrorKindSourceRead)
	}
	if eventError.Operation != "read_source" {
		t.Errorf("operation = %q; want read_source", eventError.Operation)
	}
	if !eventError.Fatal {
		t.Error("source read error was not fatal")
	}
	if eventError.Position != dataSize {
		t.Errorf("position = %d; want %d", eventError.Position, dataSize)
	}
	if eventError.Total != dataSize {
		t.Errorf("total = %d; want %d", eventError.Total, dataSize)
	}
	if eventError.BufferSize == 0 {
		t.Error("error has no buffer size context")
	}
	if !errors.Is(eventError, errSource) {
		t.Errorf("structured error does not wrap the source error: %v", eventError)
	}
	t.Logf("structured error kind=%s operation=%s position=%d total=%d buffer=%d",
		eventError.Kind, eventError.Operation, eventError.Position, eventError.Total, eventError.BufferSize)
	if strings.Contains(eventError.Detail, "alice") || strings.Contains(eventError.Detail, "top-secret") {
		t.Errorf("error detail was not redacted: %q", eventError.Detail)
	}
	if err := p.Err(); !errors.Is(err, errSource) {
		t.Errorf("Err() = %v; want it to wrap the source error", err)
	}
}

type observationBlockingCloser struct {
	started  chan struct{}
	closed   chan struct{}
	closeOne sync.Once
	readDone atomic.Bool
}

func (r *observationBlockingCloser) Read([]byte) (int, error) {
	select {
	case <-r.started:
	default:
		close(r.started)
	}
	<-r.closed
	r.readDone.Store(true)
	return 0, io.EOF
}

func (r *observationBlockingCloser) Close() error {
	r.closeOne.Do(func() {
		close(r.closed)
	})
	return nil
}

func TestObservationCancellation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		cancel func(context.CancelFunc, *mux.Player)
	}{
		{
			name: "context",
			cancel: func(cancel context.CancelFunc, _ *mux.Player) {
				cancel()
			},
		},
		{
			name: "player",
			cancel: func(_ context.CancelFunc, p *mux.Player) {
				if err := p.Cancel(); err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("Cancel() = %v; want context.Canceled", err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			src := &observationBlockingCloser{
				started: make(chan struct{}),
				closed:  make(chan struct{}),
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			m := mux.New(48000, 1, mux.FormatUnsignedInt8)
			defer m.Stop()
			p, err := m.NewPlayerWithOptions(src, &mux.NewPlayerOptions{
				Observation: &mux.ObservationOptions{Context: ctx},
			})
			if err != nil {
				t.Fatal(err)
			}
			p.Play()
			select {
			case <-src.started:
			case <-time.After(2 * time.Second):
				t.Fatal("source Read did not start")
			}

			start := time.Now()
			tt.cancel(cancel, p)
			deadline := time.Now().Add(2 * time.Second)
			for {
				if err := p.Err(); err != nil {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("Err() = %v; want context.Canceled", err)
					}
					var eventError *mux.PlayerError
					if !errors.As(err, &eventError) || eventError.Kind != mux.ErrorKindCanceled {
						t.Fatalf("Err() = %v; want a canceled structured error", err)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("canceled player did not report Err")
				}
				time.Sleep(time.Millisecond)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Errorf("cancellation took %s", elapsed)
			}
			t.Logf("cancellation reported in %s", time.Since(start))
			deadline = time.Now().Add(time.Second)
			for !src.readDone.Load() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !src.readDone.Load() {
				t.Error("source Read was still in flight after cancellation")
			}
			if p.IsPlaying() {
				t.Error("canceled player is still playing")
			}
		})
	}
}

type observationGatedReader struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *observationGatedReader) Read(buf []byte) (int, error) {
	r.once.Do(func() {
		close(r.started)
	})
	<-r.release
	clear(buf)
	return len(buf), nil
}

func TestObservationUnderrunLocation(t *testing.T) {
	src := &observationGatedReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	underrunCh := make(chan mux.Underrun, 1)
	p, err := m.NewPlayerWithOptions(src, &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{
			OnUnderrun: func(underrun mux.Underrun) {
				select {
				case underrunCh <- underrun:
				default:
				}
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Advance the mix position before the player contributes any samples.
	m.ReadFloat32s(make([]float32, 16))
	p.Play()
	select {
	case <-src.started:
	case <-time.After(2 * time.Second):
		t.Fatal("source Read did not start")
	}

	const requestedSamples = 8
	m.ReadFloat32s(make([]float32, requestedSamples))
	select {
	case underrun := <-underrunCh:
		t.Logf("underrun mix_frame=%d requested_frames=%d shortfall_frames=%d",
			underrun.MixFrame, underrun.RequestedFrames, underrun.ShortfallFrames)
		if underrun.PlayerID != 1 {
			t.Errorf("player ID = %d; want 1", underrun.PlayerID)
		}
		if underrun.MixSample != 16 {
			t.Errorf("mix sample = %d; want 16", underrun.MixSample)
		}
		if underrun.MixFrame != 16 {
			t.Errorf("mix frame = %d; want 16", underrun.MixFrame)
		}
		if underrun.RequestedFrames != requestedSamples {
			t.Errorf("requested frames = %d; want %d", underrun.RequestedFrames, requestedSamples)
		}
		if underrun.MixedFrames != 0 || underrun.ShortfallFrames != requestedSamples {
			t.Errorf("mixed/shortfall frames = %d/%d; want 0/%d", underrun.MixedFrames, underrun.ShortfallFrames, requestedSamples)
		}
		if underrun.BufferedBytes != 0 {
			t.Errorf("buffered bytes = %d; want 0", underrun.BufferedBytes)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an underrun")
	}

	close(src.release)
	_ = p.Close()
}

func TestObservationLongRunAndCleanup(t *testing.T) {
	tempDir := t.TempDir()
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	var callbacks atomic.Int64
	p, err := m.NewPlayerWithOptions(io.LimitReader(&constReader{value: 128}, 32<<20), &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{
			ProgressInterval: time.Millisecond,
			OnProgress: func(mux.Progress) {
				callbacks.Add(1)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBufferSize(4096)
	p.Play()

	buf := make([]float32, 512)
	for range 2000 {
		m.ReadFloat32s(buf)
	}
	time.Sleep(10 * time.Millisecond)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for range 20000 {
		m.ReadFloat32s(buf)
	}
	time.Sleep(10 * time.Millisecond)
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	if grew := int64(after.HeapAlloc) - int64(before.HeapAlloc); grew > 4<<20 {
		t.Errorf("heap grew by %d bytes during a long run; want no unbounded growth", grew)
	}
	t.Logf("long-run heap before=%d after=%d callbacks=%d", before.HeapAlloc, after.HeapAlloc, callbacks.Load())
	if callbacks.Load() == 0 {
		t.Error("no progress callbacks were delivered during the long run")
	}
	_ = p.Close()

	errorPlayer, err := m.NewPlayerWithOptions(&errorReader{err: errors.New("failed at /home/alice/secret")}, &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{},
	})
	if err != nil {
		t.Fatal(err)
	}
	errorPlayer.Play()
	deadline := time.Now().Add(2 * time.Second)
	for errorPlayer.Err() == nil && time.Now().Before(deadline) {
		m.ReadFloat32s(buf)
		time.Sleep(time.Millisecond)
	}
	_ = errorPlayer.Close()

	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("observation left %d temporary files", len(entries))
	}
}

func TestObservationDefaultPathUnchanged(t *testing.T) {
	errSource := errors.New("source failed")
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	p := newPlayer(t, m, &errorReader{err: errSource})
	if p.ObservationEnabled() {
		t.Error("NewPlayer enabled observation")
	}
	if _, ok := p.Progress(); ok {
		t.Error("NewPlayer returned progress")
	}
	p.Play()
	deadline := time.Now().Add(2 * time.Second)
	for p.Err() == nil && time.Now().Before(deadline) {
		m.ReadFloat32s(make([]float32, 16))
		time.Sleep(time.Millisecond)
	}
	err := p.Err()
	if !errors.Is(err, errSource) {
		t.Fatalf("Err() = %v; want the source error", err)
	}
	var eventError *mux.PlayerError
	if errors.As(err, &eventError) {
		t.Fatalf("default path returned a structured error: %v", err)
	}
}

func TestObservationLogsAreRedacted(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var logs lockedBuffer
		logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		m := mux.New(48000, 1, mux.FormatUnsignedInt8)
		defer m.Stop()

		p, err := m.NewPlayerWithOptions(bytes.NewReader(make([]byte, 8)), &mux.NewPlayerOptions{
			Observation: &mux.ObservationOptions{Logger: logger},
		})
		if err != nil {
			t.Fatal(err)
		}
		p.Play()
		deadline := time.Now().Add(2 * time.Second)
		for p.IsPlaying() && time.Now().Before(deadline) {
			m.ReadFloat32s(make([]float32, 8))
			time.Sleep(time.Millisecond)
		}
		_ = p.Close()

		waitForLog(t, &logs, "oto: player observation finished")
		text := logs.String()
		if got := strings.Count(text, "oto: player observation started"); got != 1 {
			t.Errorf("start log count = %d; want 1\n%s", got, text)
		}
		if got := strings.Count(text, "oto: player observation finished"); got != 1 {
			t.Errorf("finish log count = %d; want 1\n%s", got, text)
		}
	})

	t.Run("failure", func(t *testing.T) {
		var logs lockedBuffer
		logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
		m := mux.New(48000, 1, mux.FormatUnsignedInt8)
		defer m.Stop()

		errSource := errors.New("open /home/alice/private/audio.pcm?token=top-secret: permission denied")
		p, err := m.NewPlayerWithOptions(&errorReader{err: errSource}, &mux.NewPlayerOptions{
			Observation: &mux.ObservationOptions{Logger: logger},
		})
		if err != nil {
			t.Fatal(err)
		}
		p.Play()
		deadline := time.Now().Add(2 * time.Second)
		for p.Err() == nil && time.Now().Before(deadline) {
			m.ReadFloat32s(make([]float32, 8))
			time.Sleep(time.Millisecond)
		}
		_ = p.Close()

		waitForLog(t, &logs, "oto: player observation failed")
		text := logs.String()
		if got := strings.Count(text, "oto: player observation started"); got != 1 {
			t.Errorf("start log count = %d; want 1\n%s", got, text)
		}
		if got := strings.Count(text, "oto: player observation failed"); got != 1 {
			t.Errorf("failure log count = %d; want 1\n%s", got, text)
		}
		if !strings.Contains(text, "level=ERROR") {
			t.Errorf("failure was not logged at ERROR level\n%s", text)
		}
		for _, secret := range []string{"/home/alice", "top-secret", "permission denied"} {
			if strings.Contains(text, secret) {
				t.Errorf("log contains sensitive text %q\n%s", secret, text)
			}
		}
	})
}

func waitForLog(t *testing.T, logs *lockedBuffer, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), message) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for log %q:\n%s", message, logs.String())
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
