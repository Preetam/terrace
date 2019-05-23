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
	"bytes"
	"encoding/json"
	"io/ioutil"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func readEvents(file string) ([]Event, error) {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	events := []Event{}
	for _, eventBytes := range bytes.Split(b, []byte("\n")) {
		e := Event{}
		if len(eventBytes) == 0 {
			continue
		}
		err = json.Unmarshal(bytes.TrimSpace(eventBytes), &e)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, nil
}

func compareEvents(original, generated []Event) (bool, []byte) {
	originalLines := []string{}
	generatedLines := []string{}
	for _, e := range original {
		originalLines = append(originalLines, toJSON(e))
	}
	for _, e := range generated {
		generatedLines = append(generatedLines, toJSON(e))
	}
	sort.Strings(originalLines)
	sort.Strings(generatedLines)
	originalLinesString := strings.Join(originalLines, "\n")
	generatedLinesString := strings.Join(generatedLines, "\n")
	return originalLinesString == generatedLinesString, []byte(generatedLinesString)
}

func TestPermutate(t *testing.T) {
	cs := columnset{"a", "b", "c"}
	expected := []columnset{
		{"a", "b", "c"},
		{"b", "a", "c"},
		{"c", "a", "b"},
		{"a", "c", "b"},
		{"b", "c", "a"},
		{"c", "b", "a"},
	}
	permutations := cs.permutate(0)
	if !reflect.DeepEqual(permutations, expected) {
		t.Errorf("expected permutations %v but got %v", expected, permutations)
	}
}

func TestLossless(t *testing.T) {
	testFiles := []string{
		"simple",
	}

	for _, testFile := range testFiles {
		events, err := readEvents("./_testdata/" + testFile + ".txt")
		if err != nil {
			t.Fatal(err)
		}
		level, err := Generate(nil, events, nil, Options{Fast: true, CostType: CostTypeSize})
		if err != nil {
			t.Fatal(err)
		}
		if equal, b := compareEvents(events, level.RawEvents()); !equal {
			t.Error("events are not equal")
			ioutil.WriteFile("./_testdata/"+testFile+"_generated.txt", b, 0666)
		}
	}
}
