package dandelion

type JobMeta struct {
	id     int64 //must be total order
	uuid   string
	UserID string
	class  FlowClass
	data   []byte
}

func (jm *JobMeta) GetOffset() int64 {
	return jm.id
}

func (jm *JobMeta) GetUUID() string {
	return jm.uuid
}
