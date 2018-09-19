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
	"fmt"
	"strings"
)

// Event represents an event.
type Event map[string]interface{}

// CloneWithout clones an event without a field.
func (e Event) CloneWithout(field string) Event {
	e2 := Event{}
	for k, v := range e {
		if k != field {
			e2[k] = v
		}
	}
	return e2
}

// Level is a Terrace level.
type Level struct {
	// Column this level splits on
	Column string `json:"column,omitempty"`
	// Range of values covered by this level
	Range          ColumnRange            `json:"range,omitempty"`
	SublevelColumn string                 `json:"sublevel_column,omitempty"`
	SubLevels      []*Level               `json:"sublevels,omitempty"`
	Events         []Event                `json:"events,omitempty"`
	Fixed          map[string]interface{} `json:"fixed,omitempty"`
	Count          int                    `json:"count"`
	Sums           map[string]float64     `json:"sums,omitempty"`
}

// Push pushes an event into the level.
func (l *Level) Push(event Event, sublevels []string, columnRanges map[string][]ColumnRange) {
	l.Count++
	for k, v := range event {
		switch v.(type) {
		case int, float64:
			if l.Sums == nil {
				l.Sums = map[string]float64{}
			}
		}

		switch v.(type) {
		case int:
			l.Sums[k] += float64(v.(int))
		case float64:
			l.Sums[k] += v.(float64)
		}
	}

	if len(sublevels) == 0 {
		l.Events = append(l.Events, event)
		return
	}

	l.SublevelColumn = sublevels[0]

	// Check if sublevel column exists
	if _, ok := event[l.SublevelColumn]; !ok {
		// Nope
		l.Events = append(l.Events, event)
		return
	}

	// Create sublevels if we need to
	if len(l.SubLevels) == 0 {
		for _, r := range columnRanges[l.SublevelColumn] {
			l.SubLevels = append(l.SubLevels, &Level{Column: l.SublevelColumn, Range: r})
		}
	}

	for _, sublevel := range l.SubLevels {
		if sublevel.Range.Contains(event[l.SublevelColumn]) {
			if sublevel.Range.Single() {
				event = event.CloneWithout(l.SublevelColumn)
			}
			sublevel.Push(event, sublevels[1:], columnRanges)
			return
		}
	}
	panic("couldn't find a sublevel")
}

// Trim flattens a level and removes any unnecessary sublevels.
func (l *Level) Trim() {
	subLevelsToKeep := []*Level{}
	for _, sublevel := range l.SubLevels {
		if sublevel.Count > 0 {
			subLevelsToKeep = append(subLevelsToKeep, sublevel)
			sublevel.Trim()
		}
	}
	l.SubLevels = subLevelsToKeep
	if len(l.SubLevels) == 1 && l.SubLevels[0].Count == l.Count && l.SubLevels[0].Range.Single() {
		if l.Fixed == nil {
			l.Fixed = map[string]interface{}{}
		}
		l.Fixed[l.SublevelColumn] = l.SubLevels[0].Range.MinValue()
		l.Events = append(l.Events, l.SubLevels[0].Events...)
		for k, v := range l.SubLevels[0].Fixed {
			l.Fixed[k] = v
		}
		l.SublevelColumn = l.SubLevels[0].SublevelColumn
		l.SubLevels = l.SubLevels[0].SubLevels
	}

	eventsToRemove := 0
	for i, e := range l.Events {
		if len(e) == 0 {
			eventsToRemove++
			l.Events[i], l.Events[len(l.Events)-1] = l.Events[len(l.Events)-1], l.Events[i]
		}
	}
	l.Events = l.Events[0 : len(l.Events)-eventsToRemove]
}

func (l *Level) String() string {
	result := fmt.Sprintf("Level (%s)[%v] [%v]", l.Column, l.Range, l.Events)
	result += "\n"

	for _, sublevel := range l.SubLevels {
		result += "\t" + strings.Replace(sublevel.String(), "\n", "\n\t", -1)
		result += "\n"
	}
	return result
}

// ColumnRange represents a range of values for a column.
type ColumnRange interface {
	Contains(v interface{}) bool
	// Whether this range represents a single value
	Single() bool
	MinValue() interface{}
}

// IntegerColumnRange is an int column range.
type IntegerColumnRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

// MinValue returns the min value in the range (inclusive).
func (r IntegerColumnRange) MinValue() interface{} {
	return r.Min
}

// Contains returns true if the range may contain v.
func (r IntegerColumnRange) Contains(v interface{}) bool {
	n, ok := v.(int)
	if ok {
		return r.Min <= n && n <= r.Max
	}
	return false
}

// Single returns true if the range represents a single value.
func (r IntegerColumnRange) Single() bool {
	return r.Min == r.Max
}

// FloatColumnRange is a float64 column range.
type FloatColumnRange struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

// MinValue returns the min value in the range (inclusive).
func (r FloatColumnRange) MinValue() interface{} {
	return r.Min
}

// Contains returns true if the range may contain v.
func (r FloatColumnRange) Contains(v interface{}) bool {
	n, ok := v.(float64)
	if ok {
		return r.Min <= n && n <= r.Max
	}
	return false
}

// Single returns true if the range represents a single value.
func (r FloatColumnRange) Single() bool {
	return r.Min == r.Max
}

// StringColumnRange is a string column range.
type StringColumnRange struct {
	Min string `json:"min"`
	Max string `json:"max"`
}

// MinValue returns the min value in the range (inclusive).
func (r StringColumnRange) MinValue() interface{} {
	return r.Min
}

// Contains returns true if the range may contain v.
func (r StringColumnRange) Contains(v interface{}) bool {
	s, ok := v.(string)
	if ok {
		return r.Min <= s && s <= r.Max
	}
	return false
}

// Single returns true if the range represents a single value.
func (r StringColumnRange) Single() bool {
	return r.Min == r.Max
}

// ConstraintCondition represents a constraint condition.
type ConstraintCondition string

const (
	// ConstraintConditionEquals is an equals condition.
	ConstraintConditionEquals ConstraintCondition = "="
	// ConstraintConditionNotEquals is a not equals condition.
	ConstraintConditionNotEquals = "!="
)

// Constraint represents a constraint for a particular column.
type Constraint struct {
	Column    string              `json:"column"`
	Condition ConstraintCondition `json:"condition"`
	Value     interface{}         `json:"value"`
}

// ConstraintSet is a set of constraints for a number of columns.
type ConstraintSet map[string][]Constraint

// CheckLevel returns false if the level doesn't meet
// the constraints in the ConstraintSet.
func (cs ConstraintSet) CheckLevel(level *Level) bool {
	columnConstraints := cs[level.Column]
	for _, cons := range columnConstraints {
		if level.Range.Contains(cons.Value) {
			if cons.Condition == ConstraintConditionNotEquals {
				return false
			}
		} else {
			if cons.Condition == ConstraintConditionEquals {
				return false
			}
		}
	}
	return true
}
