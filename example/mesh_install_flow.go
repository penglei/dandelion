package main

import (
	"fmt"
	"git.code.oa.com/tke/theflow"
	"github.com/pborman/uuid"
	"log"
	"time"
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

	installMeshFlow := &theflow.FlowScheme{
		Name:       FlowClassInstall,
		NewStorage: func() interface{} { return &InstallMeshStorage{} },
		Tasks:      theflow.NewChainedTasks([]TaskScheme{t1, t2}),
	}

	theflow.Register(installMeshFlow)

}

type customData struct {
	Foo string
	Bar int
}

type K8sService interface {
	GetCluster(name string) string
}

type fakeK8sService struct {
}

func NewFakeK8sService() fakeK8sService {
	return fakeK8sService{}
}

func (f fakeK8sService) GetCluster(name string) string {
	return "fake: " + name
}

var _ K8sService = &fakeK8sService{}

type meshInstallJob struct {
	Data   customData
	K8sSvc K8sService
}

func (mj *meshInstallJob) FirstTask(ctx Context) error {
	meshName := "mesh-" + uuid.New()
	log.Println("first task running: " + meshName)
	storage := ctx.Global().(*InstallMeshStorage)
	storage.MeshName = meshName
	log.Printf("FirstTask read job data: %v", mj.Data)
	mj.Data.Bar = 456
	time.Sleep(time.Second * 2)
	return ctx.Save()
}

func (mj *meshInstallJob) SecondTask(ctx Context) error {
	storage := ctx.Global().(*InstallMeshStorage)
	log.Printf("run second task, istio: %s\n", storage.MeshName)
	log.Printf("SecondTask read job data: %v", mj.Data)
	log.Printf("calling K8sService GetCluster result: %s\n", mj.K8sSvc.GetCluster(storage.MeshName))
	return fmt.Errorf("custom error in second")
}
