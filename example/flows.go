package main

import (
	"fmt"
	"github.com/pborman/uuid"
	"log"
	"theflow"
)

const (
	FlowClassInstall = theflow.FlowClass("istio_install")
)

type Context = theflow.Context
type TaskScheme = theflow.TaskScheme
type TaskFn = theflow.TaskFn

type InstallMeshStorage struct {
	MeshName  string `json:"meshName"`
	MeshTitle string `json:"meshTitle"`
	TkeName   string `json:"tkeName,omitempty"`
}

func RegisterJobFlow(job *meshInstallJob) {
	t1 := TaskScheme{
		Name: "first",
		Task: TaskFn(job.FirstTask),
	}
	t2 := TaskScheme{
		Name: "second",
		Task: TaskFn(job.SecondTask),
	}

	tasks := theflow.NewChainedTasks([]TaskScheme{t1, t2})

	installMeshFlow := &theflow.FlowScheme{
		Name:       FlowClassInstall,
		NewStorage: func() interface{} { return &InstallMeshStorage{} },
		Tasks:      tasks,
	}

	theflow.Register(installMeshFlow)

}

func init() {
	jobEntry := &meshInstallJob{
		data: customData{
			Foo: "some_config_here",
			Bar: 123,
		},
	}
	RegisterJobFlow(jobEntry)
}

type customData struct {
	Foo string
	Bar int
}

type k8sService interface {
}

type meshInstallJob struct {
	data customData
	k8ss k8sService
}

func (mj *meshInstallJob) FirstTask(ctx *Context) error {
	meshName := "mesh-" + uuid.New()
	log.Println("first task running: " + meshName)
	storage := ctx.Global().(*InstallMeshStorage)
	storage.MeshName = meshName
	log.Printf("FirstTask read job data: %v", mj.data)
	mj.data.Bar = 456
	return ctx.Save()
}

func (mj *meshInstallJob) SecondTask(ctx *Context) error {
	storage := ctx.Global().(*InstallMeshStorage)
	log.Printf("run second task, istio: %s\n", storage.MeshName)
	log.Printf("SecondTask read job data: %v", mj.data)
	return fmt.Errorf("custom error in second")
}
