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

package oto_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ebitengine/oto/v3"
)

type observationErrorReader struct {
	err error
}

func (r *observationErrorReader) Read([]byte) (int, error) {
	return 0, r.err
}

func TestPublicObservationError(t *testing.T) {
	errSource := errors.New("source failed")
	errorCh := make(chan oto.PlayerError, 1)
	p, err := theContext.NewPlayerWithOptions(&observationErrorReader{err: errSource}, &oto.NewPlayerOptions{
		Observation: &oto.ObservationOptions{
			OnError: func(eventError oto.PlayerError) {
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

	select {
	case eventError := <-errorCh:
		if eventError.Kind != oto.ErrorKindSourceRead {
			t.Errorf("error kind = %q; want %q", eventError.Kind, oto.ErrorKindSourceRead)
		}
		if !errors.Is(eventError.Err, errSource) {
			t.Errorf("structured error does not wrap the source error: %v", eventError)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a structured error")
	}
	if !errors.Is(p.Err(), errSource) {
		t.Errorf("Err() = %v; want the source error", p.Err())
	}
}

func TestPublicObservationCancellation(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()

	p, err := theContext.NewPlayerWithOptions(r, &oto.NewPlayerOptions{
		Observation: &oto.ObservationOptions{},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Play()
	time.Sleep(20 * time.Millisecond)
	if err := p.Cancel(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Cancel() = %v; want context.Canceled", err)
	}

	err = p.Err()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Err() = %v; want context.Canceled", err)
	}
	var eventError *oto.PlayerError
	if !errors.As(err, &eventError) {
		t.Fatalf("Err() = %v; want a structured error", err)
	}
	if eventError.Kind != oto.ErrorKindCanceled {
		t.Errorf("error kind = %q; want %q", eventError.Kind, oto.ErrorKindCanceled)
	}
}

func TestPublicObservationDefaults(t *testing.T) {
	p, err := theContext.NewPlayerWithOptions(io.LimitReader(nil, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Progress(); ok {
		t.Error("nil options enabled progress")
	}
	p.Play()
	for p.IsPlaying() {
		time.Sleep(time.Millisecond)
	}
}
