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

package oto

import (
	stdcontext "context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/ebitengine/oto/v3/internal/mux"
)

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

	// ErrorKindDevice reports an audio device failure.
	ErrorKindDevice ErrorKind = "device"

	// ErrorKindDependency reports a missing or incompatible driver dependency.
	ErrorKindDependency ErrorKind = "dependency"
)

// Progress reports how much of a player's source has been accepted for
// playback. Position and Total are in bytes. Total is -1 when the source size
// is unknown, as for a streaming source.
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

// PlayerError is a classified player error with the state of the player when
// the error occurred. Detail is redacted for logs, while Err keeps the original
// error for errors.Is and errors.As.
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

// Error implements error.
func (e *PlayerError) Error() string {
	detail := e.Detail
	if detail == "" {
		detail = "operation failed"
	}
	return fmt.Sprintf("oto: audio error (%s, %s at byte %d): %s", e.Kind, e.Operation, e.Position, detail)
}

// Unwrap returns the original error.
func (e *PlayerError) Unwrap() error {
	return e.Err
}

// Underrun describes a place where a playing player had no buffered samples
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
	// could not keep up.
	Coalesced uint64
}

// ContextError is a classified audio driver error. Context.Err keeps returning
// the original error; use Context.ErrorInfo when a stable classification and
// redacted detail are useful.
type ContextError struct {
	Kind      ErrorKind
	Operation string
	Detail    string
	Err       error
	At        time.Time
}

// Error implements error.
func (e *ContextError) Error() string {
	return fmt.Sprintf("oto: %s error (%s): %s", e.Kind, e.Operation, e.Detail)
}

// Unwrap returns the original error.
func (e *ContextError) Unwrap() error {
	return e.Err
}

// ErrorInfo returns a classified view of Context.Err without changing the
// error returned by Context.Err.
func (c *Context) ErrorInfo() *ContextError {
	err := c.Err()
	if err == nil {
		return nil
	}
	return &ContextError{
		Kind:      classifyContextError(err),
		Operation: "audio_driver",
		Detail:    mux.SanitizeErrorText(err.Error()),
		Err:       err,
		At:        time.Now(),
	}
}

func classifyContextError(err error) ErrorKind {
	for current := err; current != nil; current = errors.Unwrap(current) {
		message := strings.ToLower(current.Error())
		for _, dependency := range []string{
			"library not found",
			"shared library",
			"command not found",
			"no such file",
			"failed to load",
			"dlopen",
			"undefined symbol",
			"too old",
			"unsupported version",
			"incompatible version",
		} {
			if strings.Contains(message, dependency) {
				return ErrorKindDependency
			}
		}
	}
	return ErrorKindDevice
}

// ObservationOptions enables runtime observation of a player. Omitting it
// keeps the existing behavior.
type ObservationOptions struct {
	// Context cancels the player when it is done. A nil context does not cancel.
	Context stdcontext.Context

	// ProgressInterval is the minimum interval between progress callbacks.
	// Zero disables progress sampling.
	ProgressInterval time.Duration

	// OnProgress is called with the latest progress sample. It is never called
	// from the audio mixing path. A panic is logged and does not stop playback.
	OnProgress func(Progress)

	// OnError is called with a classified operation or fatal player error.
	// A panic is logged and does not stop playback.
	OnError func(PlayerError)

	// OnUnderrun is called when a playing player has no data for an output
	// block. Slow callbacks cause later events to be coalesced.
	OnUnderrun func(Underrun)

	// Logger receives lifecycle, failure, and debug event logs. A nil logger
	// disables logging.
	Logger *slog.Logger

	// InterruptRead can interrupt a source that cannot otherwise be
	// interrupted. Closers and read deadlines are handled automatically.
	InterruptRead func() error
}

// NewPlayerOptions configures a player created by NewPlayerWithOptions.
type NewPlayerOptions struct {
	Observation *ObservationOptions
}

// NewPlayerWithOptions creates a player with optional runtime observation.
// With nil options or nil observation, it behaves exactly like NewPlayer.
//
// Progress callbacks are asynchronous and may be coalesced when a callback is
// slower than ProgressInterval. Cancellation marks Err before waiting for an
// in-flight source operation, so callers can observe cancellation promptly.
func (c *Context) NewPlayerWithOptions(r io.Reader, options *NewPlayerOptions) (*Player, error) {
	if options == nil || options.Observation == nil {
		return c.NewPlayer(r), nil
	}
	if options.Observation.ProgressInterval < 0 {
		return nil, fmt.Errorf("oto: progress interval must not be negative: %s", options.Observation.ProgressInterval)
	}

	observation := options.Observation
	muxOptions := &mux.ObservationOptions{
		Context:          observation.Context,
		ProgressInterval: observation.ProgressInterval,
		Logger:           observation.Logger,
		InterruptRead:    observation.InterruptRead,
	}
	if observation.OnProgress != nil {
		muxOptions.OnProgress = func(progress mux.Progress) {
			observation.OnProgress(fromMuxProgress(progress))
		}
	}
	if observation.OnError != nil {
		muxOptions.OnError = func(eventError *mux.PlayerError) {
			observation.OnError(fromMuxPlayerError(eventError))
		}
	}
	if observation.OnUnderrun != nil {
		muxOptions.OnUnderrun = func(underrun mux.Underrun) {
			observation.OnUnderrun(fromMuxUnderrun(underrun))
		}
	}

	player, err := c.context.mux.NewPlayerWithOptions(r, &mux.NewPlayerOptions{
		Observation: muxOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("oto: new observed player: %w", err)
	}
	return &Player{player: player}, nil
}

func fromMuxProgress(progress mux.Progress) Progress {
	return Progress{
		PlayerID:    progress.PlayerID,
		Position:    progress.Position,
		Total:       progress.Total,
		Frames:      progress.Frames,
		TotalFrames: progress.TotalFrames,
		At:          progress.At,
	}
}

func fromMuxPlayerError(eventError *mux.PlayerError) PlayerError {
	return PlayerError{
		Kind:          ErrorKind(eventError.Kind),
		Operation:     eventError.Operation,
		Position:      eventError.Position,
		Total:         eventError.Total,
		BufferedBytes: eventError.BufferedBytes,
		BufferSize:    eventError.BufferSize,
		Fatal:         eventError.Fatal,
		Detail:        eventError.Detail,
		Err:           eventError.Err,
		At:            eventError.At,
	}
}

func fromMuxUnderrun(underrun mux.Underrun) Underrun {
	return Underrun{
		PlayerID:        underrun.PlayerID,
		MixSample:       underrun.MixSample,
		MixFrame:        underrun.MixFrame,
		RequestedFrames: underrun.RequestedFrames,
		MixedFrames:     underrun.MixedFrames,
		ShortfallFrames: underrun.ShortfallFrames,
		SourcePosition:  underrun.SourcePosition,
		Total:           underrun.Total,
		BufferedBytes:   underrun.BufferedBytes,
		At:              underrun.At,
		Coalesced:       underrun.Coalesced,
	}
}
