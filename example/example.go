package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pborman/uuid"
	"github.com/penglei/dandelion"
	"go.uber.org/zap"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}
var onlyOneSignalHandler = make(chan struct{})

// SetupSignalHandler registered for SIGTERM and SIGINT. A stop channel is returned
// which is closed on one of these signals. If a second signal is caught, the program
// is terminated with exit code 1.
func SetupSignalHandler() (stopCh <-chan struct{}) {
	close(onlyOneSignalHandler) // panics when called twice

	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		time.Sleep(5 * time.Second)
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

func gracefulShutdownContext(stopCh <-chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stopCh
		cancel()
	}()
	return ctx
}

type MySQLOptions struct {
	Host     string
	Port     int
	Username string
	Password string
	Name     string
	Charset  string
}

const (
	RoleProducer = 1 + iota
	RoleConsumer
)

func main() {

	m := MySQLOptions{
		Host:     "127.0.0.1",
		Port:     3306,
		Username: "root",
		Password: "",
		Name:     "tke_mesh_trial",
	}
	userInfo := m.Username
	//var dsn = fmt.Sprintf("%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=UTC&time_zone=%s", userInfo, m.Host, m.Port, m.Name, m.Charset, url.QueryEscape(`"+00:00"`))

	dsn := fmt.Sprintf("%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4", userInfo, m.Host, m.Port, m.Name)

	fmt.Println(dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	role := RoleProducer
	if len(os.Args) >= 2 {
		if os.Args[1] == "consume" {
			role = RoleConsumer
		}
	}
	conf := zap.NewDevelopmentConfig()
	conf.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	l, err := conf.Build()
	zap.ReplaceGlobals(l)

	switch role {
	case RoleProducer:
		ctx := context.Background()
		//ctx, cancelFn := context.WithCancel(ctx)
		user := "user_default"
		if len(os.Args) >= 3 {
			user = os.Args[2]
		}

		meshStorage := InstallingStorage{
			MeshTitle: "test mesh installing",
		}
		runtime := dandelion.NewDefaultRuntime("", db)
		_, err = runtime.Submit(ctx, user, TestInstall, meshStorage)
		//runtime.Find()
		if err != nil {
			panic(err)
		}
	case RoleConsumer:
		stopCh := SetupSignalHandler()
		ctx := gracefulShutdownContext(stopCh)

		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()

		var agentName string
		if len(os.Args) >= 3 {
			agentName = os.Args[2]
		}

		runtime := dandelion.NewDefaultRuntime(agentName, db)
		err = runtime.Bootstrap(ctx)
		if err != nil {
			panic(err)
		}
		<-stopCh
		//cancelFn()
	}
}

func init() {
	job := &meshInstalling{
		Data: customData{
			Foo: "some_config_here",
			Bar: 123,
		},
		K8sSvc: NewFakeK8sService(),
	}
	registerMeshInstallJob(job)
}

const (
	TestInstall = dandelion.ProcessClass("istio_install")
)

type Context = dandelion.Context
type TaskScheme = dandelion.TaskScheme
type TaskFn = dandelion.TaskFn

type InstallingStorage struct {
	MeshName    string `json:"meshName"`
	MeshTitle   string `json:"meshTitle"`
	ClusterName string `json:"clusterName,omitempty"`
}

func registerMeshInstallJob(job *meshInstalling) {
	t1 := TaskScheme{
		Name: "first",
		Task: TaskFn(job.FirstTask),
	}
	t2 := TaskScheme{
		Name: "second",
		Task: TaskFn(job.SecondTask),
	}

	installMeshProcess := &dandelion.ProcessScheme{
		Name:          TestInstall,
		Retryable:     true,
		NewStorage:    func() interface{} { return &InstallingStorage{} },
		Orchestration: dandelion.NewChain([]TaskScheme{t1, t2}),
		OnFailure: func(ctx dandelion.Context) {
			log.Printf("failure, storage:%v\n", ctx.Global())
		},
	}

	dandelion.Register(installMeshProcess)

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

type meshInstalling struct {
	Data   customData
	K8sSvc K8sService
}

func (mj *meshInstalling) FirstTask(ctx Context) error {
	storage := ctx.Global().(*InstallingStorage)
	meshName := "mesh-" + uuid.New()
	storage.MeshName = meshName
	log.Printf("FirstTask running, storage: %v, data: %v \n", storage, mj.Data)
	mj.Data.Bar = 456
	log.Printf("FirstTask set data: %v", mj.Data)
	//panic("FirstTask panic!")
	//time.Sleep(time.Second * 2)
	return ctx.Save()
}

func (mj *meshInstalling) SecondTask(ctx Context) error {
	storage := ctx.Global().(*InstallingStorage)
	log.Printf("SecondTask running, storage: %v, data: %v\n", storage, mj.Data)
	mj.K8sSvc.GetCluster(storage.MeshName)
	time.Sleep(30 * time.Second)
	//return fmt.Errorf("custom error in second")
	return nil
}
