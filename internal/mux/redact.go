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

import "regexp"

var (
	urlCredentialsPattern = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.-]*://)[^/@\s]+@`)
	credentialsPattern    = regexp.MustCompile(`(?i)\b(password|passwd|pwd|token|secret|authorization|api[_-]?key)\b(\s*[:=]\s*)([^\s,;]+)`)
	pathPattern           = regexp.MustCompile(`(?:[A-Za-z]:[\\/]|/)(?:[^\s:'"]+[\\/])+[^\s:'"]+`)
)

// SanitizeErrorText removes common credentials and absolute paths from text
// intended for logs or structured errors.
func SanitizeErrorText(text string) string {
	text = urlCredentialsPattern.ReplaceAllString(text, `${1}[REDACTED]@`)
	text = credentialsPattern.ReplaceAllString(text, `${1}${2}[REDACTED]`)
	text = pathPattern.ReplaceAllString(text, `[REDACTED_PATH]`)
	return text
}
