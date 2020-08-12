package dandelion

import (
	"encoding/json"
	"github.com/penglei/dandelion/database"
)

type planStateSerializable struct {
	SpawnedTasks map[string]taskStateSerializable `json:"spawned_tasks"`
}

type taskStateSerializable struct {
	Status database.TypeStatusRaw `json:"status"`
	Name   string                 `json:"name"`
}

func deserializePlanState(data []byte, s *PlanState) error {
	serializableState := &planStateSerializable{
		SpawnedTasks: make(map[string]taskStateSerializable),
	}
	if err := json.Unmarshal(data, serializableState); err != nil {
		return err
	}

	for name, item := range serializableState.SpawnedTasks {
		task := newTask(name, StatusFromRaw(item.Status))
		//XXX task.executed
		s.SpawnedTasks[name] = task
	}
	return nil

}

func serializePlanState(s *PlanState) ([]byte, error) {
	serializableState := planStateSerializable{
		SpawnedTasks: make(map[string]taskStateSerializable, len(s.SpawnedTasks)),
	}
	for i, task := range s.SpawnedTasks {
		taskSerializable := taskStateSerializable{
			Status: task.status.Raw(),
			Name:   task.name,
		}
		serializableState.SpawnedTasks[i] = taskSerializable
	}
	return json.Marshal(serializableState)
}

func deserializeStorage(data []byte, target interface{}) error {
	return json.Unmarshal(data, target)
}

func serializeStorage(target interface{}) ([]byte, error) {
	return json.Marshal(target)
}
