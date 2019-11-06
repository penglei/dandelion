package main

import (
	"github.com/penglei/dandelion"
	"github.com/pborman/uuid"
	"log"
	"time"
)

const (
	FlowClassInstall = dandelion.FlowClass("istio_install")
)

type Context = dandelion.Context
type TaskScheme = dandelion.TaskScheme
type TaskFn = dandelion.TaskFn

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

	installMeshFlow := &dandelion.FlowScheme{
		Name:       FlowClassInstall,
		NewStorage: func() interface{} { return &InstallMeshStorage{} },
		Tasks:      dandelion.NewChainedTasks([]TaskScheme{t1, t2}),
		OnFailure: func(ctx dandelion.Context) {
			log.Printf("failure, storage:%v\n", ctx.Global())
		},
	}

	dandelion.Register(installMeshFlow)

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
	clusterName := "fake: " + name
	log.Printf("Calling K8sService GetCluster: %s --> %s\n", name, clusterName)
	return clusterName
}

var _ K8sService = &fakeK8sService{}

type meshInstallJob struct {
	Data   customData
	K8sSvc K8sService
}

func (mj *meshInstallJob) FirstTask(ctx Context) error {
	storage := ctx.Global().(*InstallMeshStorage)
	meshName := "mesh-" + uuid.New()
	storage.MeshName = meshName
	log.Printf("FirstTask running, storage: %v, data: %v \n", storage, mj.Data)
	mj.Data.Bar = 456
	log.Printf("FirstTask set data: %v", mj.Data)
	//panic("FirstTask panic!")
	time.Sleep(time.Second * 2)
	return ctx.Save()
}

func (mj *meshInstallJob) SecondTask(ctx Context) error {
	storage := ctx.Global().(*InstallMeshStorage)
	log.Printf("SecondTask running, storage: %v, data: %v\n", storage, mj.Data)
	mj.K8sSvc.GetCluster(storage.MeshName)
	//return fmt.Errorf("custom error in second")
	return nil
}
