package terrace

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
