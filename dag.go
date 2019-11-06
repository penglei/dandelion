package dandelion

type taskNode struct {
	Follows []*taskNode
}
type DAGFlowTask struct {
	Heads []*taskNode
}

func (d *DAGFlowTask) Prepare(state *FlowExecPlanState) {
	panic("implement me")
}

func (d *DAGFlowTask) Restore(state *FlowExecPlanState) error {
	panic("implement me")
}

func (d *DAGFlowTask) Next() []*Task {
	panic("implement me")
}
func (d *DAGFlowTask) Update([]*Task) {
	panic("implement me")
}

var _ TaskOrchestration = &DAGFlowTask{}
