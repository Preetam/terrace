package terrace

const (
	// CostLevel is the cost for a level access.
	CostLevel = 1000
	// CostEvent is the cost for an event access.
	CostEvent = 1
)

func calculateCost(level *Level, cs ConstraintSet) int {
	if !cs.CheckLevel(level) {
		// Doesn't meet constraints; skipped.
		return 0
	}
	cost := 0
	for _, sublevel := range level.SubLevels {
		cost += CostLevel + calculateCost(sublevel, cs)
	}
	cost += CostEvent * len(level.Events)
	return cost
}
