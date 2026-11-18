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
	"testing"
	"time"

	"github.com/ebitengine/oto/v3/internal/mux"
)

func BenchmarkObservedPlayerMix(b *testing.B) {
	m := mux.New(48000, 1, mux.FormatUnsignedInt8)
	defer m.Stop()

	p, err := m.NewPlayerWithOptions(&constReader{value: 128}, &mux.NewPlayerOptions{
		Observation: &mux.ObservationOptions{
			ProgressInterval: time.Millisecond,
			OnProgress:       func(mux.Progress) {},
			OnUnderrun:       func(mux.Underrun) {},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	p.SetBufferSize(4096)
	p.Play()
	defer p.Close()

	buf := make([]float32, 512)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		m.ReadFloat32s(buf)
	}
}
