// Name: Abraham Zamora
// Date: 10/28/2024
// Assignment #3

package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Define the Node structure for the Binary Search Tree (BST)
type Node struct {
	key   int
	left  *Node
	right *Node
}

// Define the Tree structure, which contains a pointer to the root node
type Tree struct {
	root      *Node
	id        int
	hashValue int
}

// Inserts a value into the tree. This will be the root if the tree is empty. If not, it will call the helper function to insert the value
func (t *Tree) Insert(key int) {
	if t.root == nil {
		t.root = &Node{key: key}
	} else {
		t.root.insertHelper(key)
	}
}

// Helper function to insert new nodes into the tree. It uses recursion
func (n *Node) insertHelper(key int) {
	if key < n.key {
		if n.left == nil {
			n.left = &Node{key: key}
		} else {
			n.left.insertHelper(key)
		}
	} else if key > n.key {
		if n.right == nil {
			n.right = &Node{key: key}
		} else {
			n.right.insertHelper(key)
		}
	}
}

// In order traversal of the tree. Returns a slice of the values in the tree. Relies on the helper function to do the recursion
func (t *Tree) InOrderTraverse() []int {
	var result []int
	if t.root == nil {
		return result
	} else {
		return t.root.inOrderTraverseHelper()
	}
}

// Helper function to do the recursion for the in-order traversal
func (n *Node) inOrderTraverseHelper() []int {
	var result []int
	if n.left != nil {
		result = append(result, n.left.inOrderTraverseHelper()...)
	}
	result = append(result, n.key)
	if n.right != nil {
		result = append(result, n.right.inOrderTraverseHelper()...)
	}
	return result
}

// Hash function
func hash(values []int) int {
	hash := 1
	for _, value := range values {
		new_value := value + 2
		hash = (hash*new_value + new_value) % 1000
	}
	return hash
}

// Parsing the line from the input file into a slice of integers
func parseLine(line string) []int {
	elements := strings.Split(line, " ")
	var values []int
	for _, element := range elements {
		value, _ := strconv.Atoi(element)
		values = append(values, value)
	}
	return values
}

// Create a BST from the slice of integers
func createBST(values []int, id int) *Tree {
	tree := &Tree{id: id}
	for _, value := range values {
		tree.Insert(value)
	}
	return tree
}

// Read the file and parse the lines into a slice of integers
func readInputFile(inputFilePath string) ([][]int, error) {
	file, err := os.Open(inputFilePath)
	if err != nil {
		return nil, fmt.Errorf("Error: could not open file %s", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var values [][]int
	for scanner.Scan() {
		line := scanner.Text()
		values = append(values, parseLine(line))
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("Error reading file: %v", err)
	}

	return values, nil
}

// Compare two in-order traversals to check if the trees are identical
func identicalTraversals(t1, t2 []int) bool {
	if len(t1) != len(t2) {
		return false
	}
	for i := range t1 {
		if t1[i] != t2[i] {
			return false
		}
	}
	return true
}

// Hashing Functions
func sequentialHash(trees []*Tree) map[int][]*Tree {
	hashResults := make(map[int][]*Tree)
	for _, tree := range trees {
		inOrderValues := tree.InOrderTraverse()
		tree.hashValue = hash(inOrderValues)
		hashResults[tree.hashValue] = append(hashResults[tree.hashValue], tree)
	}
	return hashResults
}

func parallelHashingWithChannels(trees []*Tree, numWorkers int) map[int][]*Tree {
	hashResults := make(map[int][]*Tree)
	var mu sync.Mutex
	hashChan := make(chan map[int][]*Tree, numWorkers)

	worker := func(start, end, workerID int, wg *sync.WaitGroup) {
		defer wg.Done()
		localHashResults := make(map[int][]*Tree) // Local results for each worker

		// for debugging, track the range of trees each worker is processing
		//fmt.Printf("Worker %d processing trees from index %d to %d\n", workerID, start, end)

		for i := start; i < end; i++ {
			inOrderValues := trees[i].InOrderTraverse()
			trees[i].hashValue = hash(inOrderValues)
			localHashResults[trees[i].hashValue] = append(localHashResults[trees[i].hashValue], trees[i])
		}
		// Send local results to the channel instead of directly modifying the global map
		hashChan <- localHashResults
	}

	// Manager goroutine to collect results from the channel and update the global map
	go func() {
		for i := 0; i < numWorkers; i++ {
			localResults := <-hashChan
			mu.Lock()
			for k, v := range localResults {
				hashResults[k] = append(hashResults[k], v...)
			}
			mu.Unlock()
		}
	}()

	wg := &sync.WaitGroup{}
	chunkSize := len(trees) / numWorkers

	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(trees) // Ensure the last worker processes the remainder
		}
		wg.Add(1)
		go worker(start, end, i, wg)
	}

	wg.Wait()
	close(hashChan) // Close the channel when all workers are done
	return hashResults
}

func parallelHashingAtomic(trees []*Tree, numWorkers int) map[int][]*Tree {
	var mu sync.Mutex
	hashResults := make(map[int][]*Tree)

	// Worker function with local map to reduce contention
	worker := func(start, end, workerID int, wg *sync.WaitGroup) {
		defer wg.Done()
		localHashResults := make(map[int][]*Tree)
		//fmt.Printf("Worker %d processing trees parrHash from index %d to %d\n", workerID, start, end)

		for i := start; i < end; i++ {
			inOrderValues := trees[i].InOrderTraverse()
			trees[i].hashValue = hash(inOrderValues)
			localHashResults[trees[i].hashValue] = append(localHashResults[trees[i].hashValue], trees[i])
		}
		// Lock and update the global map in one go
		mu.Lock()
		for k, v := range localHashResults {
			hashResults[k] = append(hashResults[k], v...)
		}
		mu.Unlock()
	}

	// Create goroutines based on numWorkers
	wg := &sync.WaitGroup{}
	chunkSize := len(trees) / numWorkers
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(trees) // Ensure the last chunk processes the remainder
		}
		wg.Add(1)
		go worker(start, end, i, wg)
	}

	wg.Wait()
	return hashResults
}

// Optional: Implementing parallel with fine-grained locks
func parallelHashingOptional(trees []*Tree, hashWorkers int, dataWorkers int) map[int][]*Tree {
	hashResults := make(map[int][]*Tree)
	locks := make(map[int]*sync.Mutex) // Mutex for each hash entry
	var mu sync.Mutex                  // Mutex for accessing the locks map and protecting hashResults
	dataChannels := make([]chan *Tree, dataWorkers)

	// Initialize channels for data workers
	for i := 0; i < dataWorkers; i++ {
		dataChannels[i] = make(chan *Tree, len(trees)/dataWorkers)
	}

	// Data worker function to update the map using fine-grained locks
	dataWorker := func(workerID int, wg *sync.WaitGroup) {
		defer wg.Done()
		for tree := range dataChannels[workerID] {
			// Acquire lock for this particular hash entry
			mu.Lock()
			if _, exists := locks[tree.hashValue]; !exists {
				// Create the lock for this hash value if it doesn't exist
				locks[tree.hashValue] = &sync.Mutex{}
			}
			lock := locks[tree.hashValue]
			mu.Unlock()

			// Lock only this particular hash value entry
			lock.Lock()
			// Make sure access to the hashResults map is thread-safe
			hashResults[tree.hashValue] = append(hashResults[tree.hashValue], tree)
			lock.Unlock()
		}
	}

	// Hash worker function to compute hash values and send trees to data workers
	hashWorker := func(start, end int, workerID int, wg *sync.WaitGroup) {
		defer wg.Done()
		for i := start; i < end; i++ {
			inOrderValues := trees[i].InOrderTraverse()
			trees[i].hashValue = hash(inOrderValues)
			// Send each tree to one of the data workers based on round-robin distribution
			dataChannels[i%dataWorkers] <- trees[i]
		}
	}

	// Start data workers
	wgData := &sync.WaitGroup{}
	for i := 0; i < dataWorkers; i++ {
		wgData.Add(1)
		go dataWorker(i, wgData)
	}

	// Start hash workers
	wgHash := &sync.WaitGroup{}
	chunkSize := len(trees) / hashWorkers
	for i := 0; i < hashWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == hashWorkers-1 {
			end = len(trees) // Ensure the last worker processes the remainder
		}
		wgHash.Add(1)
		go hashWorker(start, end, i, wgHash)
	}

	// Wait for all hash workers to finish
	wgHash.Wait()

	// Close the channels to signal data workers to stop
	for i := 0; i < dataWorkers; i++ {
		close(dataChannels[i])
	}

	// Wait for all data workers to finish
	wgData.Wait()

	return hashResults
}

// Grouping Functions
func sequentialGroup(trees []*Tree) map[int][]*Tree {
	groupResults := make(map[int][]*Tree)
	for _, tree := range trees {
		groupResults[tree.hashValue] = append(groupResults[tree.hashValue], tree)
	}
	return groupResults
}

func parallelGroupWithChannels(trees []*Tree, numWorkers int) map[int][]*Tree {
	groupResults := make(map[int][]*Tree)
	groupChan := make(chan *Tree, len(trees))

	// Worker function to process trees and send to groupChan
	worker := func(treeChan chan *Tree, done chan bool) {
		for tree := range treeChan {
			groupChan <- tree
		}
		done <- true // Signal that worker is done
	}

	// Create a channel to send trees to workers
	treeChan := make(chan *Tree, len(trees))
	done := make(chan bool)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		go worker(treeChan, done)
	}

	// Send trees to workers
	for _, tree := range trees {
		treeChan <- tree
	}
	close(treeChan) // Close treeChan after sending all trees

	// Wait for all workers to complete
	for i := 0; i < numWorkers; i++ {
		<-done
	}

	// Close groupChan after all trees are processed
	close(groupChan)

	// Collect results from the groupChan
	for tree := range groupChan {
		groupResults[tree.hashValue] = append(groupResults[tree.hashValue], tree)
	}

	return groupResults
}

func parallelGroupAtomic(trees []*Tree, numWorkers int) map[int][]*Tree {
	var mu sync.Mutex
	groupResults := make(map[int][]*Tree)

	// Worker function to group the trees
	worker := func(start, end int, wg *sync.WaitGroup) {
		defer wg.Done()
		for i := start; i < end; i++ {
			mu.Lock()
			groupResults[trees[i].hashValue] = append(groupResults[trees[i].hashValue], trees[i])
			mu.Unlock()
		}
	}

	wg := &sync.WaitGroup{}
	chunkSize := len(trees) / numWorkers
	// Distribute the work among the workers
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(trees)
		}
		wg.Add(1)
		go worker(start, end, wg)
	}

	wg.Wait()
	return groupResults
}

// optional: Grouping with fine-grained locks

func parallelGroupingOptional(trees []*Tree, groupWorkers int) map[int][]*Tree {
	groupResults := make(map[int][]*Tree)
	locks := make(map[int]*sync.Mutex)       // Mutex for each hash entry
	var mu sync.Mutex                        // Mutex for accessing the locks map
	treeChan := make(chan *Tree, len(trees)) // Channel to distribute trees to workers

	// Data worker function to update the map using fine-grained locks
	groupWorker := func(workerID int, wg *sync.WaitGroup) {
		defer wg.Done()
		for tree := range treeChan {
			// Acquire lock for this particular hash entry
			mu.Lock()
			if _, exists := locks[tree.hashValue]; !exists {
				locks[tree.hashValue] = &sync.Mutex{}
			}
			lock := locks[tree.hashValue]
			mu.Unlock()

			// Lock only this particular hash value entry
			lock.Lock()
			groupResults[tree.hashValue] = append(groupResults[tree.hashValue], tree)
			lock.Unlock()
		}
	}

	// Start group workers
	wg := &sync.WaitGroup{}
	for i := 0; i < groupWorkers; i++ {
		wg.Add(1)
		go groupWorker(i, wg)
	}

	// Send all trees to the workers via the channel
	for _, tree := range trees {
		treeChan <- tree
	}
	close(treeChan) // Close the channel to signal workers to stop

	// Wait for all group workers to finish
	wg.Wait()

	return groupResults
}

// Compare the trees - Sequential
func compareIdenticalTreesSequential(treeGroup map[int][]*Tree) [][]int {
	groupIndex := 0
	visited := make(map[int]bool)
	var groupedTrees [][]int // To store groups of identical trees

	for _, trees := range treeGroup {
		for i := 0; i < len(trees); i++ {
			if visited[trees[i].id] {
				continue // Skip trees that have already been grouped
			}
			group := []int{trees[i].id}
			visited[trees[i].id] = true
			for j := i + 1; j < len(trees); j++ {
				if identicalTraversals(trees[i].InOrderTraverse(), trees[j].InOrderTraverse()) {
					group = append(group, trees[j].id)
					visited[trees[j].id] = true
				}
			}
			if len(group) > 1 {
				groupedTrees = append(groupedTrees, group)
				groupIndex++
			}
		}
	}
	return groupedTrees
}

func compareIdenticalTreesParallel(treeGroup map[int][]*Tree, compWorkers int) [][]int {
	var groupedTrees [][]int
	var mu sync.Mutex

	// Channel for worker pool
	tasks := make(chan []*Tree, len(treeGroup))

	// Launch worker pool
	var wg sync.WaitGroup
	for w := 0; w < compWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for trees := range tasks {
				// Process each tree group
				visited := make(map[int]bool)
				localGroups := [][]int{}

				for i := 0; i < len(trees); i++ {
					if visited[trees[i].id] {
						continue
					}

					group := []int{trees[i].id}
					visited[trees[i].id] = true
					baseTraversal := trees[i].InOrderTraverse()

					for j := i + 1; j < len(trees); j++ {
						if !visited[trees[j].id] && identicalTraversals(baseTraversal, trees[j].InOrderTraverse()) {
							group = append(group, trees[j].id)
							visited[trees[j].id] = true
						}
					}

					if len(group) > 1 {
						localGroups = append(localGroups, group)
					}
				}

				// Add local groups to global result
				if len(localGroups) > 0 {
					mu.Lock()
					groupedTrees = append(groupedTrees, localGroups...)
					mu.Unlock()
				}
			}
		}()
	}

	// Send work to workers
	for _, trees := range treeGroup {
		if len(trees) > 1 {
			tasks <- trees
		}
	}
	close(tasks)
	wg.Wait()

	return groupedTrees
}

// compare trees with one goroutine per comparison
func compareIdenticalTreesOneGoRoutinePerComparison(treeGroup map[int][]*Tree) [][]int {
	var wg sync.WaitGroup
	visited := make(map[int]bool)
	var mu sync.Mutex        // Mutex to protect concurrent access to `visited` and `groupedTrees`
	var groupedTrees [][]int // To store groups of identical trees

	for _, trees := range treeGroup {
		for i := 0; i < len(trees); i++ {
			mu.Lock()
			if visited[trees[i].id] {
				mu.Unlock()
				continue // Skip trees that have already been grouped
			}
			mu.Unlock()

			group := []int{trees[i].id}
			mu.Lock()
			visited[trees[i].id] = true
			mu.Unlock()

			for j := i + 1; j < len(trees); j++ {
				wg.Add(1)
				go func(i, j int) {
					defer wg.Done()

					if identicalTraversals(trees[i].InOrderTraverse(), trees[j].InOrderTraverse()) {
						mu.Lock()
						group = append(group, trees[j].id)
						visited[trees[j].id] = true
						mu.Unlock()
					}
				}(i, j)
			}

			wg.Wait() // Wait for all comparisons for `trees[i]` to complete

			mu.Lock()
			if len(group) > 1 {
				groupedTrees = append(groupedTrees, group)
			}
			mu.Unlock()
		}
	}
	return groupedTrees
}

func main() {
	// Parse command-line arguments
	hashWorker := flag.Int("hash-workers", 0, "integer-valued number of threads")
	dataWorker := flag.Int("data-workers", 0, "integer-valued number of threads")
	compWorker := flag.Int("comp-workers", 0, "integer-valued number of threads")
	inputFile := flag.String("input", "", "string-valued path to an input file")

	flag.Parse()

	if *inputFile == "" {
		log.Fatalf("Error: no input file provided")
	}

	// Create the full file path using the 'input' folder (corrected for final input)
	inputFilePath := *inputFile

	// Check if the input file exists
	if _, err := os.Stat(inputFilePath); os.IsNotExist(err) {
		log.Fatalf("Error: input file does not exist at path %s", inputFilePath)
	}

	// Process the input file and generate trees
	parsedLines, err := readInputFile(inputFilePath)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	trees := []*Tree{}
	for i, values := range parsedLines {
		tree := createBST(values, i)
		trees = append(trees, tree)
	}

	var treeGroup map[int][]*Tree

	// Hashing section
	start := time.Now()
	if *hashWorker == 0 {
		// One goroutine per BST
		var wg sync.WaitGroup
		treeGroup = make(map[int][]*Tree)
		var mu sync.Mutex

		for _, tree := range trees {
			wg.Add(1)
			go func(tree *Tree) {
				defer wg.Done()
				inOrderValues := tree.InOrderTraverse()
				tree.hashValue = hash(inOrderValues)

				mu.Lock()
				treeGroup[tree.hashValue] = append(treeGroup[tree.hashValue], tree)
				mu.Unlock()
			}(tree)
		}
		wg.Wait()
	} else if *hashWorker == 1 && (*dataWorker == 1) {
		treeGroup = sequentialHash(trees)
	} else if *hashWorker > 1 && *dataWorker == 1 {
		treeGroup = parallelHashingWithChannels(trees, *hashWorker)
	} else if (*hashWorker > 1 && *dataWorker > 1) && (*hashWorker == *dataWorker) {
		treeGroup = parallelHashingAtomic(trees, *hashWorker)
	} else if (*hashWorker > 1 && *dataWorker > 1) && (*hashWorker > *dataWorker) {
		treeGroup = parallelHashingOptional(trees, *hashWorker, *dataWorker)
	} else if *hashWorker > 1 {
		treeGroup = parallelHashingWithChannels(trees, *hashWorker)
	} else if *hashWorker == 1 {
		treeGroup = sequentialHash(trees)
	} else {
		log.Fatalf("Error: invalid number of workers")
	}
	end := time.Since(start).Seconds()
	fmt.Printf("hashTime: %v\n", end)

	// Only proceed with grouping if dataWorker is specified
	if *dataWorker > 0 {
		startGroup := time.Now()

		if *hashWorker == 0 {
			// One goroutine per BST for grouping
			var wg sync.WaitGroup
			treeGroup = make(map[int][]*Tree)
			var mu sync.Mutex

			for _, tree := range trees {
				wg.Add(1)
				go func(tree *Tree) {
					defer wg.Done()
					mu.Lock()
					treeGroup[tree.hashValue] = append(treeGroup[tree.hashValue], tree)
					mu.Unlock()
				}(tree)
			}
			wg.Wait()
		} else if *hashWorker == 1 && *dataWorker == 1 {
			treeGroup = sequentialGroup(trees)
		} else if *hashWorker > 1 && (*dataWorker == 0 || *dataWorker == 1) {
			treeGroup = parallelGroupWithChannels(trees, *hashWorker)
		} else if (*hashWorker > 1 && *dataWorker > 1) && (*hashWorker == *dataWorker) {
			treeGroup = parallelGroupAtomic(trees, *dataWorker)
		} else if (*hashWorker > 1 && *dataWorker > 1) && (*hashWorker > *dataWorker) {
			treeGroup = parallelGroupingOptional(trees, *dataWorker)
		}

		hashGroupDuration := time.Since(startGroup).Seconds()
		fmt.Printf("hashGroupTime: %v\n", hashGroupDuration)

		// Print the hash groups
		for hashValue, groupedTrees := range treeGroup {
			fmt.Printf("%d:", hashValue)
			for _, tree := range groupedTrees {
				fmt.Printf(" %d", tree.id)
			}
			fmt.Println()
		}

		// Only proceed with comparison if compWorker is specified
		if *compWorker > 0 {
			startCompare := time.Now()
			var groupedTrees [][]int

			if *compWorker == 0 {
				groupedTrees = compareIdenticalTreesOneGoRoutinePerComparison(treeGroup)
			} else if *compWorker == 1 {
				groupedTrees = compareIdenticalTreesSequential(treeGroup)
			} else if *compWorker > 1 {
				groupedTrees = compareIdenticalTreesParallel(treeGroup, *compWorker)
			}

			compareDuration := time.Since(startCompare).Seconds()
			fmt.Printf("compareTreeTime: %v\n", compareDuration)

			for i, group := range groupedTrees {
				fmt.Printf("group %d:", i)
				for _, id := range group {
					fmt.Printf(" %d", id)
				}
				fmt.Println()
			}
		}
	}
}
