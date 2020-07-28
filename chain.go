package dandelion

import (
	"errors"
)

//Chain creates a chain of tasks to be processed one by one
type Chain struct {
	nextIndex   int       //running task nextIndex cursor
	tasks       []*RtTask //spawned tasks
	nameSchemes map[string]*TaskScheme
	schemes     []*TaskScheme
}

func (c *Chain) Prepare(pstate *PlanState) {
	c.nextIndex = 0
}

func (c *Chain) Snapshot() []*RtTask {
	return c.tasks
}

func (c *Chain) Restore(pstate *PlanState) error {
	spawnedSize := len(pstate.SpawnedTasks)

	/*
		//TODO validate
		if len(c.schemes) != spawnedSize {
			return fmt.Errorf("process scheme:%s has been changed", schemeName)
		}
	*/

	for i := 0; i < spawnedSize; i += 1 {
		scheme := c.schemes[i]
		task := pstate.SpawnedTasks[scheme.Name]
		task.setScheme(scheme)
		c.tasks = append(c.tasks, task)
	}

	cursorSetFlag := false
	for i, task := range c.tasks {
		if task.status == StatusPending || task.status == StatusRunning {
			c.nextIndex = i
			cursorSetFlag = true
			break
		}
	}

	if !cursorSetFlag {
		c.nextIndex = len(c.tasks)
	}

	return nil
}

func (c *Chain) Next() []*RtTask {
	nextIdx := c.nextIndex

	taskCnt := len(c.schemes)

	// no more task
	if nextIdx > taskCnt-1 {
		return nil
	}

	lastSpawnedTask := len(c.tasks) - 1
	if nextIdx > lastSpawnedTask {
		// new tasks must be generated
		scheme := c.schemes[nextIdx]
		task := newTask(scheme.Name, StatusPending)
		task.setScheme(scheme)

		c.tasks = append(c.tasks, task)
	}
	tasks := c.tasks[nextIdx : nextIdx+1]
	c.nextIndex = nextIdx + 1

	//TODO
	//1. if task status is Blocked
	//	--> call task Resume method.
	return tasks
}

func (c *Chain) Update(tasks []*RtTask) {
	// needn't, these tasks have been append to spawned tasks when were generating
	//c.tasks = append(c.tasks, tasks...)
	//c.nextIndex += len(tasks)
}

func validateTaskSchemes(taskSchemes []TaskScheme) error {
	names := make(map[string]struct{})
	for _, item := range taskSchemes {
		if _, ok := names[item.Name]; !ok {
			names[item.Name] = struct{}{}
		} else {
			return errors.New("duplicate task name: " + item.Name)
		}
	}

	return nil
}

func NewChain(taskSchemes []TaskScheme) func() TaskOrchestration {
	if err := validateTaskSchemes(taskSchemes[:]); err != nil {
		panic(err)
	}

	nameSchemes := make(map[string]*TaskScheme, len(taskSchemes))
	schemes := make([]*TaskScheme, len(taskSchemes))

	for i, item := range taskSchemes {
		item := item //copy it
		nameSchemes[item.Name] = &item
		schemes[i] = &item
	}

	return func() TaskOrchestration {
		return &Chain{
			nextIndex:   -1,
			schemes:     schemes,
			nameSchemes: nameSchemes,
			tasks:       make([]*RtTask, 0),
		}
	}
}

var _ TaskOrchestration = &Chain{}
