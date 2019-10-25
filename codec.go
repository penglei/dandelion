package theflow

import (
	"encoding/json"
	"theflow/database"
)

type flowStateSerializable struct {
	SpawnedTasks map[string]taskStateSerializable `json:"spawned_tasks"`
	Error        string                           `json:"error,omitempty"`
}

type taskStateSerializable struct {
	Status database.TypeStatusRaw `json:"status"`
	Name   string                 `json:"name"`
}

func deserializeInternalState(data []byte, s *FlowInternalState) error {
	serializableState := &flowStateSerializable{
		SpawnedTasks: make(map[string]taskStateSerializable),
	}
	if err := json.Unmarshal(data, serializableState); err != nil {
		return err
	}

	for name, item := range serializableState.SpawnedTasks {
		s.SpawnedTasks[name] = newTask(name, StatusFromRaw(item.Status))
	}
	return nil

}

func serializeInternalState(s *FlowInternalState) ([]byte, error) {

	serializableState := flowStateSerializable{
		SpawnedTasks: make(map[string]taskStateSerializable, len(s.SpawnedTasks)),
	}

	for i, task := range s.SpawnedTasks {
		taskSerializable := taskStateSerializable{
			Status: task.status.Raw(),
			Name:   task.name,
		}
		serializableState.SpawnedTasks[i] = taskSerializable
	}

	if s.Error != nil {
		serializableState.Error = s.Error.Error()
	}

	return json.Marshal(serializableState)
}

func deserializeStorage(data []byte, target interface{}) error {
	return json.Unmarshal(data, target)
}

func serializeStorage(target interface{}) ([]byte, error) {
	return json.Marshal(target)
}
