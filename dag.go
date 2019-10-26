package theflow

type taskNode struct {
	Nexis []*taskNode
}
type DAGFlowTask struct {
	Heads []*taskNode
}

func (d *DAGFlowTask) Prepare(state *FlowInternalState) {
	panic("implement me")
}

func (d *DAGFlowTask) Restore(state *FlowInternalState) error {
	panic("implement me")
}

func (d *DAGFlowTask) Next() []*Task {
	panic("implement me")
}
func (d *DAGFlowTask) Update([]*Task) {
	panic("implement me")
}

var _ TaskOrchestration = &DAGFlowTask{}
