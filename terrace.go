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

	"github.com/Preetam/query"
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

// Fields returns a slice of fields within this event.
func (e Event) Fields() []string {
	fields := []string{}
	for k := range e {
		fields = append(fields, k)
	}
	return fields
}

// Get gets the value for field from the event. False
// is returned if the field doesn't exist.
func (e Event) Get(field string) (interface{}, bool) {
	v, ok := e[field]
	return v, ok
}

// Event satisfies the query.Row interface.
var _ query.Row = Event{}

// Level is a Terrace level.
type Level struct {
	// Column this level splits on
	Column string `json:"column,omitempty"`
	// Range of values covered by this level
	Range          JSONColumnRange        `json:"range,omitempty"`
	InternalRange  ColumnRange            `json:"-"`
	SublevelColumn string                 `json:"sublevel_column,omitempty"`
	Sublevels      []*Level               `json:"sublevels,omitempty"`
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
	if len(l.Sublevels) == 0 {
		for _, r := range columnRanges[l.SublevelColumn] {
			sublevel := &Level{Column: l.SublevelColumn, InternalRange: r}
			switch r.(type) {
			case IntegerColumnRange:
				sublevel.Range = JSONColumnRange{
					Type: "int",
					Min:  r.(IntegerColumnRange).Min,
					Max:  r.(IntegerColumnRange).Max,
				}
			case FloatColumnRange:
				sublevel.Range = JSONColumnRange{
					Type: "float",
					Min:  r.(FloatColumnRange).Min,
					Max:  r.(FloatColumnRange).Max,
				}
			case StringColumnRange:
				sublevel.Range = JSONColumnRange{
					Type: "string",
					Min:  r.(StringColumnRange).Min,
					Max:  r.(StringColumnRange).Max,
				}
			}
			l.Sublevels = append(l.Sublevels, sublevel)
		}
	}

	for _, sublevel := range l.Sublevels {
		if sublevel.InternalRange.Contains(event[l.SublevelColumn]) {
			if sublevel.InternalRange.Single() {
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
	for _, sublevel := range l.Sublevels {
		if sublevel.Count > 0 {
			subLevelsToKeep = append(subLevelsToKeep, sublevel)
			sublevel.Trim()
		}
	}
	l.Sublevels = subLevelsToKeep
	if len(l.Sublevels) == 1 && l.Sublevels[0].Count == l.Count && l.Sublevels[0].InternalRange.Single() {
		if l.Fixed == nil {
			l.Fixed = map[string]interface{}{}
		}
		l.Fixed[l.SublevelColumn] = l.Sublevels[0].InternalRange.MinValue()
		l.Events = append(l.Events, l.Sublevels[0].Events...)
		for k, v := range l.Sublevels[0].Fixed {
			l.Fixed[k] = v
		}
		l.SublevelColumn = l.Sublevels[0].SublevelColumn
		l.Sublevels = l.Sublevels[0].Sublevels
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
	return l.string(0)
}

func (l *Level) string(indent int) string {
	indentString := strings.Repeat("\t", indent)
	numEventsString := ""
	if len(l.Events) > 0 {
		numEventsString = fmt.Sprintf(" %d events", len(l.Events))
	}
	fixedValuesString := ""
	if len(l.Fixed) > 0 {
		fixedValuesString = fmt.Sprintf(" fixed: %v", l.Fixed)
	}

	result := ""
	if l.Column == "" {
		// Base
		result = fmt.Sprintf("Base%s%s\n", numEventsString, fixedValuesString)
	} else {
		rangeString := fmt.Sprint(l.InternalRange)
		if l.InternalRange.Single() {
			rangeString = fmt.Sprintf("{%v}", l.InternalRange.MinValue())
		}
		result = indentString + fmt.Sprintf("%s => %s %s%s\n", l.Column, rangeString, numEventsString, fixedValuesString)
	}
	for _, sublevel := range l.Sublevels {
		result += sublevel.string(indent+1) + "\n"
	}
	result = strings.TrimRight(result, "\n")
	return result
}

// RawEvents returns the raw events represented by this level.
func (l *Level) RawEvents() []Event {
	events := make([]Event, 0, l.Count)
	events = append(events, l.Events...)
	for _, subLevel := range l.Sublevels {
		events = append(events, subLevel.RawEvents()...)
	}
	for len(events) != l.Count {
		events = append(events, Event{})
	}
	for i := range events {
		for k, v := range l.Fixed {
			events[i][k] = v
		}
	}
	return events
}

func (l *Level) NewCursor() (query.Cursor, error) {
	return &LevelCursor{
		events: l.RawEvents(),
	}, nil
}

type LevelCursor struct {
	events []Event
}

func (cur *LevelCursor) Row() query.Row {
	if len(cur.events) > 0 {
		return cur.events[0]
	}
	return nil
}

func (cur *LevelCursor) Next() bool {
	if len(cur.events) == 0 {
		return false
	}
	cur.events = cur.events[1:]
	return len(cur.events) > 0
}

func (cur *LevelCursor) Err() error {
	return nil
}

var _ query.Cursor = &LevelCursor{}

// ColumnRange represents a range of values for a column.
type ColumnRange interface {
	Contains(v interface{}) bool
	// Whether this range represents a single value
	Single() bool
	MinValue() interface{}
}

type JSONColumnRange struct {
	Type string      `json:"type"`
	Min  interface{} `json:"min"`
	Max  interface{} `json:"max"`
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

// ConstraintOperator represents a constraint operator.
type ConstraintOperator string

const (
	// ConstraintOperatorEquals is an equals operator.
	ConstraintOperatorEquals ConstraintOperator = "="
	// ConstraintOperatorNotEquals is a not equals operator.
	ConstraintOperatorNotEquals = "!="
)

// Constraint represents a constraint for a particular column.
type Constraint struct {
	Column   string             `json:"column"`
	Operator ConstraintOperator `json:"operator"`
	Value    interface{}        `json:"value"`
}

// ConstraintSet is a set of constraints for a number of columns.
type ConstraintSet map[string][]Constraint

// CheckLevel returns false if the level doesn't meet
// the constraints in the ConstraintSet.
func (cs ConstraintSet) CheckLevel(level *Level) bool {
	columnConstraints := cs[level.Column]
	for _, cons := range columnConstraints {
		if level.InternalRange.Contains(cons.Value) {
			if cons.Operator == ConstraintOperatorNotEquals {
				return false
			}
		} else {
			if cons.Operator == ConstraintOperatorEquals {
				return false
			}
		}
	}
	return true
}

var _ query.Table = &Level{}
