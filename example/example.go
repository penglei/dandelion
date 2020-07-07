package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/penglei/dandelion"
	"go.uber.org/zap"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
)

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
		Name:     "tke_mesh",
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

	ctx := context.Background()
	//ctx, cancelFn := context.WithCancel(ctx)

	switch role {
	case RoleProducer:
		user := "user_default"
		if len(os.Args) >= 3 {
			user = os.Args[2]
		}

		meshStorage := InstallMeshStorage{
			MeshTitle: "test mesh installing",
		}
		flowRuntime := dandelion.NewDefaultRuntime("", db)
		err = flowRuntime.Submit(ctx, user, FlowClassInstall, meshStorage)
		if err != nil {
			panic(err)
		}
	case RoleConsumer:
		go func() {
			log.Println(http.ListenAndServe("localhost:6060", nil))
		}()

		var agentName string
		if len(os.Args) >= 3 {
			agentName = os.Args[2]
		}

		flowRuntime := dandelion.NewDefaultRuntime(agentName, db)
		err = flowRuntime.Bootstrap(ctx)
		if err != nil {
			panic(err)
		}
		stopCh := make(chan struct{})
		<-stopCh
		//cancelFn()

	}

}

func init() {
	job := &meshInstallJob{
		Data: customData{
			Foo: "some_config_here",
			Bar: 123,
		},
		K8sSvc: NewFakeK8sService(),
	}
	registerMeshInstallJob(job)
}
