package searchrequest

import (
	"container/heap"
)

// An item is something we manage in a priority queue.
type item struct {
	value    ResultRow
	priority string
	// The index is needed by update and
	// is maintained by the heap.Interface methods.
	index int
}

// priorityQueue list of pointers to item structs
type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	isLess := pq[i].priority < pq[j].priority
	return isLess
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push : pushes an item onto the priority queu
func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*item)
	item.index = n
	*pq = append(*pq, item)
}

// Pop : removes an item from the priority queue
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an item in the queue.
func (pq *priorityQueue) update(item *item, value ResultRow, priority string) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
