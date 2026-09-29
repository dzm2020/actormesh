package component

import (
	"reflect"
	"testing"
)

type priorityTestComponent struct {
	BaseComponent
	priority int
}

func newPriorityTestComponent(name string, priority int) *priorityTestComponent {
	c := &priorityTestComponent{priority: priority}
	c.SetName(name)
	return c
}

func (c *priorityTestComponent) Priority() int { return c.priority }

func TestManagerRangesComponentsByPriority(t *testing.T) {
	manager := NewComponentsMgr()
	components := []*priorityTestComponent{
		newPriorityTestComponent("late", 20),
		newPriorityTestComponent("first", -10),
		newPriorityTestComponent("middle-a", 10),
		newPriorityTestComponent("middle-b", 10),
	}
	items := make([]IComponent, len(components))
	for i, c := range components {
		items[i] = c
	}
	if err := manager.Add(items...); err != nil {
		t.Fatal(err)
	}

	var order []string
	manager.RangeInOrder(func(c IComponent) bool {
		order = append(order, c.GetName())
		return true
	})
	if want := []string{"first", "middle-a", "middle-b", "late"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("priority order = %v, want %v", order, want)
	}

	order = nil
	manager.RangeInReverseOrder(func(c IComponent) bool {
		order = append(order, c.GetName())
		return true
	})
	if want := []string{"late", "middle-b", "middle-a", "first"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("reverse priority order = %v, want %v", order, want)
	}
}
