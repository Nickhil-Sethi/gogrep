package searchrequest

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value    ResultRow
	priority string
	// The index is needed by update and
	// is maintained by the heap.Interface methods.
	index int
}

// PriorityQueue list of pointers to item structs
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	isLess := pq[i].priority < pq[j].priority
	return isLess
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push : pushes an item onto the priority queu
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

// Pop : removes an item from the priority queue
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value ResultRow, priority string) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
