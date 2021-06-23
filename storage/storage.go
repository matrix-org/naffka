// Copyright 2020 The Matrix.org Foundation C.I.C.
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

// +build !wasm

package storage

import (
	"fmt"
	"net/url"

	_ "github.com/mattn/go-sqlite3"

	"github.com/matrix-org/naffka/storage/postgres"
	"github.com/matrix-org/naffka/storage/sqlite3"
)

// NewDatabase opens a new database
func NewDatabase(dsn string) (Database, error) {
	uri, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	switch uri.Scheme {
	case "file":
		if uri.Opaque != "" { // file:filename.db
			return sqlite3.NewDatabase(uri.Opaque)
		} else if uri.Path != "" { // file:///path/to/filename.db
			return sqlite3.NewDatabase(uri.Path)
		} else {
			return nil, fmt.Errorf("unexpected file URI")
		}
	case "postgres":
		fallthrough
	case "postgresql":
		return postgres.NewDatabase(dsn)
	default:
		return nil, fmt.Errorf("unexpected database type")
	}
}
