package terrace

import "encoding/json"

/**
 * Copyright (C) 2018 Preetam Jinka
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

const (
	// CostLevel is the cost for a level access.
	CostLevel = 10
	// CostEvent is the cost for an event access.
	CostEvent = 100
)

const (
	CostTypeAccess = iota
	CostTypeSize
)

func calculateCost(costType int, level *Level, cs ConstraintSet, eventsScale float64) int {
	if costType == CostTypeAccess {
		if !cs.CheckLevel(level) {
			// Doesn't meet constraints; skipped.
			return 0
		}
		cost := 0
		for _, sublevel := range level.SubLevels {
			cost += CostLevel + calculateCost(costType, sublevel, cs, eventsScale)
		}
		cost += int(eventsScale * float64(CostEvent*len(level.Events)))
		return cost
	} else if costType == CostTypeSize {
		b, _ := json.Marshal(level)
		return len(b)
	} else {
		panic("terrace: unknown cost type")
	}
}
