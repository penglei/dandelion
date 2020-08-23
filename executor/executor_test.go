package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pborman/uuid"
	"github.com/penglei/dandelion/scheme"
	"go.uber.org/zap"
	"gotest.tools/assert"
	"log"
	"testing"
)

type memorySnapshotExporter struct {
	stores map[string]ProcessState
	data   []byte
}

func (m *memorySnapshotExporter) Write(id string, snapshot ProcessState) error {
	m.stores[id] = snapshot
	storeBytes, err := json.Marshal(m.stores)
	if err != nil {
		return err
	}
	fmt.Printf("stores: %s\n", string(storeBytes))
	m.data = storeBytes
	return nil
}

func (m *memorySnapshotExporter) Read(id string) (*ProcessState, error) {
	var a = m.stores[id]
	return &a, nil
}

func newMemorySnapshotExporter() *memorySnapshotExporter {
	return &memorySnapshotExporter{
		stores: make(map[string]ProcessState, 0),
	}
}

var _ SnapshotExporter = &memorySnapshotExporter{}

type appStorage struct {
	Count int
}

type testProcessTasks struct {
	sth string
}

var disableTestPanic = false

func (mj *testProcessTasks) FirstTask(ctx scheme.Context) error {
	storage := ctx.Global().(*appStorage)
	mj.sth = "mesh-" + uuid.New()
	log.Printf("FirstTask running, storage: %v\n", storage)
	log.Printf("FirstTask set data: %v", mj.sth)
	if !disableTestPanic {
		panic("FirstTask panic!")
	}
	storage.Count = 123
	return nil
}

func (mj *testProcessTasks) SecondTask(ctx scheme.Context) error {
	storage := ctx.Global().(*appStorage)
	log.Printf("SecondTask running, data: %v, storage: %v\n", mj.sth, storage)
	return nil
}

func registerTestProcess(name scheme.ProcessClass) {
	tasks := &testProcessTasks{}
	t1 := scheme.TaskScheme{
		Name: "first",
		Task: scheme.TaskFn(tasks.FirstTask),
	}
	t2 := scheme.TaskScheme{
		Name: "second",
		Task: scheme.TaskFn(tasks.SecondTask),
	}

	processScheme := &scheme.ProcessScheme{
		Name:       name,
		Retryable:  true,
		NewStorage: func() interface{} { return &appStorage{} },
		Tasks:      []scheme.TaskScheme{t1, t2},
		OnFailure: func(ctx scheme.Context) {
			log.Printf("failure, storage:%v\n", ctx.Global())
		},
	}

	scheme.Register(processScheme)
}

func TestRuntime(t *testing.T) {
	name := scheme.ClassFromRaw("test_process")
	registerTestProcess(name)

	zapConf := zap.NewDevelopmentConfig()
	zapConf.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	lgr, err := zapConf.Build()
	assert.NilError(t, err)
	zap.ReplaceGlobals(lgr)

	exporter := newMemorySnapshotExporter()
	pm := NewProcessManager(exporter, lgr)
	ctx := context.Background()

	processId := "aaa"
	testProcessScheme, err := scheme.Resolve(name)
	assert.NilError(t, err)
	storage := testProcessScheme.NewStorage().(*appStorage)

	err = pm.Run(ctx, processId, *testProcessScheme, storage)
	assert.NilError(t, err)

	snapshot := make(map[string]ProcessState)
	err = json.Unmarshal(exporter.data, &snapshot)
	assert.NilError(t, err)
	savedProcessSnapshot := snapshot[processId]
	fmt.Printf("%+v\n", savedProcessSnapshot.Storage)

	disableTestPanic = true

	err = pm.Retry(ctx, processId, *testProcessScheme)
	assert.NilError(t, err)
}
