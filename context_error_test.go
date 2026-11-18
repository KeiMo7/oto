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
	"errors"
	"fmt"
	"testing"
)

func TestClassifyContextError(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want ErrorKind
	}{
		{
			name: "missing dependency",
			err:  errors.New("PulseAudio: shared library not found"),
			want: ErrorKindDependency,
		},
		{
			name: "old dependency",
			err:  fmt.Errorf("backend: %w", errors.New("unsupported version 1.2")),
			want: ErrorKindDependency,
		},
		{
			name: "device",
			err:  errors.New("device disconnected"),
			want: ErrorKindDevice,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyContextError(tt.err); got != tt.want {
				t.Errorf("classifyContextError(%q) = %q; want %q", tt.err, got, tt.want)
			}
		})
	}
}
