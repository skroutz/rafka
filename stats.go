// Copyright 2017 Skroutz S.A.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package main

import (
	"strconv"
	"sync/atomic"
)

type Stats struct {
	producerUnflushed int64
	producerErr       int64
}

func (s *Stats) toRedis() []interface{} {
	return []interface{}{
		"producer.unflushed.messages",
		strconv.FormatInt(atomic.LoadInt64(&s.producerUnflushed), 10),
		"producer.delivery.errors",
		strconv.FormatInt(atomic.LoadInt64(&s.producerErr), 10),
	}
}
