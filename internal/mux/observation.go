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

package mux

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Progress reports how much of a player's source has been accepted for
// playback. Position and Total are in bytes. Total is -1 when the total size is
// unknown, as for a streaming source.
type Progress struct {
	PlayerID    uint64
	Position    int64
	Total       int64
	Frames      int64
	TotalFrames int64
	At          time.Time
}

// Ratio returns Position/Total. ok is false when the total is unknown.
func (p Progress) Ratio() (ratio float64, ok bool) {
	if p.Total <= 0 {
		return 0, false
	}
	return float64(p.Position) / float64(p.Total), true
}

// ErrorKind classifies a player error.
type ErrorKind string

const (
	// ErrorKindSourceRead is an error returned by the source's Read method.
	ErrorKindSourceRead ErrorKind = "source_read"

	// ErrorKindSeek is a failed Seek operation.
	ErrorKindSeek ErrorKind = "seek"

	// ErrorKindUnsupported is an operation the source does not support.
	ErrorKindUnsupported ErrorKind = "unsupported"

	// ErrorKindCanceled reports an explicit or context cancellation.
	ErrorKindCanceled ErrorKind = "canceled"
)

// PlayerError is a classified error with the state of the player when it
// occurred. The log-friendly Detail is redacted. Err keeps the original error
// for errors.Is and errors.As.
type PlayerError struct {
	Kind          ErrorKind
	Operation     string
	Position      int64
	Total         int64
	BufferedBytes int
	BufferSize    int
	Fatal         bool
	Detail        string
	Err           error
	At            time.Time
}

func (e *PlayerError) Error() string {
	detail := e.Detail
	if detail == "" {
		detail = "operation failed"
	}
	return fmt.Sprintf("mux: %s at byte %d: %s", e.Operation, e.Position, detail)
}

func (e *PlayerError) Unwrap() error {
	return e.Err
}

// Underrun describes a point where a playing player had no buffered samples
// for an output block. Positions identify the mix buffer, not the source.
type Underrun struct {
	PlayerID        uint64
	MixSample       int64
	MixFrame        int64
	RequestedFrames int
	MixedFrames     int
	ShortfallFrames int
	SourcePosition  int64
	Total           int64
	BufferedBytes   int
	At              time.Time

	// Coalesced counts later underruns folded into this event because callbacks
	// could not keep up. It is zero for an event delivered immediately.
	Coalesced uint64
}

// ObservationOptions enables runtime observation of a player. A nil
// ObservationOptions keeps the default behavior.
type ObservationOptions struct {
	// Context cancels the player when it is done. A nil context does not cancel.
	Context context.Context

	// ProgressInterval is the minimum interval between progress callbacks.
	// Zero disables progress sampling.
	ProgressInterval time.Duration

	// OnProgress is called with the latest progress sample. It is never called
	// from the audio mixing path.
	OnProgress func(Progress)

	// OnError is called with a classified, non-fatal operation error or a fatal
	// player error.
	OnError func(*PlayerError)

	// OnUnderrun is called when a playing player has no data for an output
	// block. Slow callbacks cause later events to be coalesced.
	OnUnderrun func(Underrun)

	// Logger receives lifecycle, failure, and debug event logs. The zero value
	// disables logging.
	Logger *slog.Logger

	// InterruptRead is called during cancellation to interrupt a source that
	// cannot otherwise be interrupted. Closers and read deadlines are handled
	// automatically. It must be safe to call concurrently with Read.
	InterruptRead func() error
}

// NewPlayerOptions configures a player created by NewPlayerWithOptions.
type NewPlayerOptions struct {
	Observation *ObservationOptions
}

type observationFinal struct {
	status string
}

type playerObservation struct {
	id            uint64
	ctx           context.Context
	interval      time.Duration
	onProgress    func(Progress)
	onError       func(*PlayerError)
	onUnderrun    func(Underrun)
	logger        *slog.Logger
	interruptRead func() error
	wantsUnderrun bool

	notify chan struct{}
	stopCh chan struct{}

	mu           sync.Mutex
	progress     *Progress
	lastProgress *Progress
	underrun     Underrun
	hasUnderrun  bool
	eventError   *PlayerError
	final        *observationFinal
	stopped      bool

	stopOnce sync.Once
}

func newPlayerObservation(id uint64, total int64, options *ObservationOptions) *playerObservation {
	o := &playerObservation{
		id:            id,
		ctx:           options.Context,
		interval:      options.ProgressInterval,
		onProgress:    options.OnProgress,
		onError:       options.OnError,
		onUnderrun:    options.OnUnderrun,
		logger:        options.Logger,
		interruptRead: options.InterruptRead,
		wantsUnderrun: options.OnUnderrun != nil || options.Logger != nil,
		notify:        make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
	}
	if o.logger != nil {
		o.logger.Info("oto: player observation started",
			"player_id", id,
			"total_bytes", total,
		)
	}
	return o
}

func (o *playerObservation) start(p *playerImpl) {
	o.setProgress(*p.progressSnapshot())
	hasContext := o.ctx != nil && o.ctx.Done() != nil
	if hasContext || o.interval > 0 {
		go o.watch(p)
	}
	if o.logger != nil || o.onProgress != nil || o.onError != nil || o.onUnderrun != nil {
		go o.publish()
	}
}

func (o *playerObservation) watch(p *playerImpl) {
	var tick <-chan time.Time
	if o.interval > 0 {
		ticker := time.NewTicker(o.interval)
		defer ticker.Stop()
		tick = ticker.C
	}

	var contextDone <-chan struct{}
	if o.ctx != nil {
		contextDone = o.ctx.Done()
	}

	for {
		select {
		case <-o.stopCh:
			return
		case <-contextDone:
			_ = p.cancel(context.Cause(o.ctx))
			return
		case <-tick:
			if progress := p.progressSnapshot(); progress != nil {
				o.setProgress(*progress)
			}
		}
	}
}

func (o *playerObservation) publish() {
	for {
		select {
		case <-o.notify:
			o.drain()
		case <-o.stopCh:
			o.drain()
			return
		}
	}
}

func (o *playerObservation) drain() {
	o.mu.Lock()
	progress := o.progress
	o.progress = nil
	underrun := o.underrun
	hasUnderrun := o.hasUnderrun
	o.hasUnderrun = false
	eventError := o.eventError
	o.eventError = nil
	final := o.final
	o.final = nil
	lastProgress := o.lastProgress
	o.mu.Unlock()

	if progress != nil && o.onProgress != nil {
		o.call("progress", func() {
			o.onProgress(*progress)
		})
	}
	if hasUnderrun {
		if o.onUnderrun != nil {
			o.call("underrun", func() {
				o.onUnderrun(underrun)
			})
		}
		if o.logger != nil {
			o.logger.Debug("oto: player underrun",
				"player_id", underrun.PlayerID,
				"mix_sample", underrun.MixSample,
				"mix_frame", underrun.MixFrame,
				"mix_frames", underrun.MixedFrames,
				"requested_frames", underrun.RequestedFrames,
				"shortfall_frames", underrun.ShortfallFrames,
				"source_position_bytes", underrun.SourcePosition,
				"total_bytes", underrun.Total,
				"buffered_bytes", underrun.BufferedBytes,
				"coalesced", underrun.Coalesced,
			)
		}
	}
	if eventError != nil {
		o.reportError(eventError)
	}
	if final != nil {
		if final.status != "failed" && o.logger != nil {
			status := final.status
			position, total := int64(0), int64(-1)
			if lastProgress != nil {
				position, total = lastProgress.Position, lastProgress.Total
			}
			o.logger.Info("oto: player observation finished",
				"player_id", o.id,
				"status", status,
				"position_bytes", position,
				"total_bytes", total,
			)
		}
	}
}

func (o *playerObservation) reportError(eventError *PlayerError) {
	if o.onError != nil {
		o.call("error", func() {
			o.onError(eventError)
		})
	}
	if o.logger != nil {
		o.logger.Error("oto: player observation failed",
			"player_id", o.id,
			"error_kind", string(eventError.Kind),
			"operation", eventError.Operation,
			"position_bytes", eventError.Position,
			"total_bytes", eventError.Total,
			"buffered_bytes", eventError.BufferedBytes,
			"buffer_size_bytes", eventError.BufferSize,
			"error_type", fmt.Sprintf("%T", eventError.Err),
		)
	}
}

func (o *playerObservation) call(name string, callback func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger := o.logger
			if logger == nil {
				logger = slog.Default()
			}
			logger.Error("oto: observation callback panicked",
				"player_id", o.id,
				"callback", name,
				"panic_type", fmt.Sprintf("%T", recovered),
			)
		}
	}()
	callback()
}

func (o *playerObservation) signal() {
	select {
	case o.notify <- struct{}{}:
	default:
	}
}

func (o *playerObservation) setProgress(progress Progress) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.stopped {
		return
	}
	p := progress
	o.progress = &p
	o.lastProgress = &p
	o.signal()
}

func (o *playerObservation) setUnderrun(underrun Underrun) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.stopped {
		return
	}
	if o.hasUnderrun {
		o.underrun.Coalesced++
	} else {
		o.underrun = underrun
		o.hasUnderrun = true
	}
	o.signal()
}

func (o *playerObservation) setError(eventError *PlayerError) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.stopped {
		return
	}
	o.eventError = eventError
	o.signal()
}

func (o *playerObservation) finish(status string, eventError *PlayerError, progress *Progress) {
	o.stopOnce.Do(func() {
		o.mu.Lock()
		if progress != nil {
			p := *progress
			o.progress = &p
			o.lastProgress = &p
		}
		if eventError != nil {
			o.eventError = eventError
		}
		o.final = &observationFinal{status: status}
		o.stopped = true
		o.mu.Unlock()

		close(o.stopCh)
		o.signal()
	})
}

func (o *playerObservation) latestProgress() (Progress, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.lastProgress == nil {
		return Progress{}, false
	}
	return *o.lastProgress, true
}

func (o *playerObservation) interruptSource() {
	if o.interruptRead != nil {
		o.call("interrupt_read", func() {
			_ = o.interruptRead()
		})
	}
}
