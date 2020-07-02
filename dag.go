package dandelion

type stepNode struct {
	Follows []*stepNode
}
type DAGFlowTask struct {
	Heads []*stepNode
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
