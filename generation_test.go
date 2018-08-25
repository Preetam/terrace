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
	"reflect"
	"testing"
)

func TestPermutate(t *testing.T) {
	cs := columnset{"a", "b", "c"}
	expected := []columnset{
		columnset{"a", "b", "c"},
		columnset{"b", "a", "c"},
		columnset{"c", "a", "b"},
		columnset{"a", "c", "b"},
		columnset{"b", "c", "a"},
		columnset{"c", "b", "a"},
	}
	permutations := cs.permutate(0)
	if !reflect.DeepEqual(permutations, expected) {
		t.Errorf("expected permutations %v but got %v", expected, permutations)
	}
}
