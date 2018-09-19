package terrace

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

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
)

func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func fromJSON(s string) interface{} {
	var val interface{}
	err := json.Unmarshal([]byte(s), &val)
	if err != nil {
		panic(err)
	}
	return val
}

// Generate generates a Level.
func Generate(logger *log.Logger, events []Event, constraints []ConstraintSet) (*Level, error) {
	var bestLevel *Level
	var bestLevelCost = int(math.MaxInt64)
	var bestColumnOrder = []string{}

	columnSet := getColumnSet(events)
	logger.Printf("Generation: Considering column set: %v", columnSet)
	orderings := columnSet.permutate(0)
	logger.Printf("Generation: %d total possible orderings", len(orderings))

	columnRanges := getColumnRangesForColumnSet(columnSet, 16, events)
	logger.Printf("Generation: Using column ranges %s", toJSON(columnRanges))

	seenOrdering := map[string]bool{}

ORDERINGS_LOOP:
	for _, allColumns := range orderings {
		if rand.Float64() > (4000 / float64(len(orderings))) {
			continue
		}
		for i := 1; i <= len(allColumns); i++ {
			columnOrder := allColumns[:i]

			if seenOrdering[strings.Join(columnOrder, "")] {
				continue
			}
			seenOrdering[strings.Join(columnOrder, "")] = true

			// Rough filter: ignore orderings that are not constrained by
			// the first column.
			skipOrdering := true
			for _, cs := range constraints {
				if _, ok := cs[columnOrder[0]]; ok {
					skipOrdering = false
					break
				}
			}
			if skipOrdering {
				continue
			}
			level := &Level{}

			for _, e := range events {
				if rand.Float64() > (1000 / float64(len(events))) {
					continue
				}
				level.Push(e, []string(columnOrder), columnRanges)
			}

			level.Trim()
			cost := 0
			for _, cs := range constraints {
				cost += calculateCost(level, cs, (float64(len(events)) / 1000))
			}
			logger.Printf("Generation: Cost %d for column order %v", cost, columnOrder)

			if cost < bestLevelCost {
				bestLevel = level
				bestLevelCost = cost
				bestColumnOrder = []string(columnOrder)
			} else {
				continue ORDERINGS_LOOP
			}
		}
	}

	logger.Printf("Generation: Best column order with cost %d: %v", bestLevelCost, bestColumnOrder)

	logger.Printf("Generation: Generating final level")
	bestLevel = &Level{}
	for _, e := range events {
		bestLevel.Push(e, bestColumnOrder, columnRanges)
	}
	logger.Printf("Generation: Trimming")
	bestLevel.Trim()
	return bestLevel, nil
}

func explode(events []Event) []string {
	entries := []string{}
	for _, e := range events {
		permutations := e.toKVs().permutate(0)
		for _, p := range permutations {
			keyStrings := []string{}
			valueStrings := []string{}
			for _, pair := range p {
				keyStrings = append(keyStrings, pair.key)
				valueStrings = append(valueStrings, toJSON(pair.value))
			}
			entries = append(entries, strings.Join(keyStrings, "\x00")+"\x00\x00"+strings.Join(valueStrings, "\x00"))
		}
	}
	sort.Strings(entries)
	return entries
}

func (e Event) toKVs() kvs {
	result := kvs{}
	for k, v := range e {
		result = append(result, kv{key: k, value: v})
	}
	return result
}

func (e Event) fromKVs(pairs kvs) {
	for _, pair := range pairs {
		e[pair.key] = pair.value
	}
}

type kv struct {
	key   string
	value interface{}
}

type kvs []kv

func (pairs kvs) swap(i, j int) {
	pairs[i], pairs[j] = pairs[j], pairs[i]
}

func (pairs kvs) permutate(n int) []kvs {
	if n == 0 {
		n = len(pairs)
	}
	results := []kvs{}
	if n == 1 {
		var newPairs kvs
		return []kvs{append(newPairs, pairs...)}
	}
	for i := 0; i < n-1; i++ {
		results = append(results, pairs.permutate(n-1)...)
		if n%2 == 0 {
			pairs.swap(i, n-1)
		} else {
			pairs.swap(0, n-1)
		}
	}
	results = append(results, pairs.permutate(n-1)...)
	return results
}

type columnset []string

// permutate returns permutations of the columnset using
// Heap's algorithm (see https://en.wikipedia.org/wiki/Heap%27s_algorithm).
func (cs columnset) permutate(n int) []columnset {
	if n == 0 {
		n = len(cs)
		if n == 0 {
			return []columnset{}
		}
	}
	results := []columnset{}
	if n == 1 {
		var newCS columnset
		return []columnset{append(newCS, cs...)}
	}
	for i := 0; i < n-1; i++ {
		results = append(results, cs.permutate(n-1)...)
		if n%2 == 0 {
			// swap i, n-1
			cs[i], cs[n-1] = cs[n-1], cs[i]
		} else {
			// swap 0, n-1
			cs[0], cs[n-1] = cs[n-1], cs[0]
		}
	}
	results = append(results, cs.permutate(n-1)...)
	return results
}

// getColumnSet returns a good columnset for the given events.
func getColumnSet(events []Event) columnset {
	intColumns := map[string]bool{}
	stringColumns := map[string]bool{}
	columnCardinality := map[string]map[string]struct{}{}
	allColumns := map[string]bool{}
	ignoredColumns := map[string]bool{}
	const maxCardinality = 2048

	for _, e := range events {
		for k, v := range e {
			if ignoredColumns[k] {
				continue
			}
			switch v.(type) {
			case string:
				if intColumns[k] {
					ignoredColumns[k] = true
					continue
				}
				stringColumns[k] = true
			case int:
				if stringColumns[k] {
					ignoredColumns[k] = true
					continue
				}
				intColumns[k] = true
				ignoredColumns[k] = true
			default:
				ignoredColumns[k] = true
				continue
			}
			allColumns[k] = true

			_, ok := columnCardinality[k]
			if !ok {
				columnCardinality[k] = map[string]struct{}{}
			}
			columnCardinality[k][fmt.Sprint(v)] = struct{}{}
			if len(columnCardinality[k]) > maxCardinality {
				ignoredColumns[k] = true
			}
		}
	}
	for _, e := range events {
		for k := range allColumns {
			if _, ok := e[k]; !ok {
				ignoredColumns[k] = true
			}
		}
	}
	cs := columnset{}
	for k := range allColumns {
		if !ignoredColumns[k] {
			cs = append(cs, k)
		}
	}

	return cs
}

func getColumnRangesForColumnSet(cs columnset, max int, events []Event) map[string][]ColumnRange {
	result := map[string][]ColumnRange{}
	for _, column := range cs {
		var vals sort.Interface
		seenInts, seenFloats, seenStrings := map[int]bool{}, map[float64]bool{}, map[string]bool{}
		switch events[0][column].(type) {
		case int:
			vals = sort.IntSlice{}
		case float64:
			vals = sort.Float64Slice{}
		case string:
			vals = sort.StringSlice{}
		}

		for _, e := range events {
			if _, ok := e[column]; !ok {
				continue
			}
			switch e[column].(type) {
			case int:
				if seenInts[e[column].(int)] {
					continue
				}
				seenInts[e[column].(int)] = true
				typedVals := vals.(sort.IntSlice)
				typedVals = append(typedVals, e[column].(int))
				vals = typedVals
			case float64:
				if seenFloats[e[column].(float64)] {
					continue
				}
				seenFloats[e[column].(float64)] = true
				typedVals := vals.(sort.Float64Slice)
				typedVals = append(typedVals, e[column].(float64))
				vals = typedVals
			case string:
				if seenStrings[e[column].(string)] {
					continue
				}
				seenStrings[e[column].(string)] = true
				typedVals := vals.(sort.StringSlice)
				typedVals = append(typedVals, e[column].(string))
				vals = typedVals
			}
		}

		sort.Sort(vals)

		switch vals.(type) {
		case sort.IntSlice:
			parts := splitIntSlice([]int(vals.(sort.IntSlice)), max)
			for _, part := range parts {
				min, max := part[0], part[len(part)-1]
				result[column] = append(result[column], IntegerColumnRange{Min: min, Max: max})
			}
		case sort.Float64Slice:
			parts := splitFloat64Slice([]float64(vals.(sort.Float64Slice)), max)
			for _, part := range parts {
				min, max := part[0], part[len(part)-1]
				result[column] = append(result[column], FloatColumnRange{Min: min, Max: max})
			}
		case sort.StringSlice:
			parts := splitStringSlice([]string(vals.(sort.StringSlice)), max)
			for _, part := range parts {
				min, max := part[0], part[len(part)-1]
				result[column] = append(result[column], StringColumnRange{Min: min, Max: max})
			}
		}
	}
	return result
}

func splitIntSlice(s []int, parts int) [][]int {
	l := len(s)
	if l < parts {
		parts = l
	}
	result := [][]int{}
	for i := 0; i < parts; i++ {
		result = append(result, s[i*(l/parts):(i+1)*(l/parts)])
	}
	if partSize := len(result[0]); partSize*parts != l {
		result[parts-1] = append(result[parts-1], s[parts*partSize:]...)
	}
	return result
}

func splitFloat64Slice(s []float64, parts int) [][]float64 {
	l := len(s)
	if l < parts {
		parts = l
	}
	result := [][]float64{}
	for i := 0; i < parts; i++ {
		result = append(result, s[i*(l/parts):(i+1)*(l/parts)])
	}
	if partSize := len(result[0]); partSize*parts != l {
		result[parts-1] = append(result[parts-1], s[parts*partSize:]...)
	}
	return result
}

func splitStringSlice(s []string, parts int) [][]string {
	l := len(s)
	if l < parts {
		parts = l
	}
	result := [][]string{}
	for i := 0; i < parts; i++ {
		result = append(result, s[i*(l/parts):(i+1)*(l/parts)])
	}
	if partSize := len(result[0]); partSize*parts != l {
		result[parts-1] = append(result[parts-1], s[parts*partSize:]...)
	}
	return result
}
