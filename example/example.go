package main

import (
	"context"
	"database/sql"
	"fmt"
	"git.code.oa.com/tke/theflow"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLOptions struct {
	Host     string
	Port     int
	Username string
	Password string
	Name     string
	Charset  string
}

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

	flowRuntime := theflow.NewDefaultRuntime("example_local", db)

	ctx := context.Background()
	ctx, cancelFn := context.WithCancel(ctx)
	err = flowRuntime.Bootstrap(ctx)
	if err != nil {
		panic(err)
	}

	createNewJob := true
	//createNewJob = false

	if createNewJob {
		user := "user_local"
		meshStorage := InstallMeshStorage{
			MeshTitle: "test_mesh_installing",
		}
		err = flowRuntime.CreateJob(ctx, user, FlowClassInstall, meshStorage)
		if err != nil {
			panic(err)
		}
	}

	stopCh := make(chan struct{})
	<-stopCh
	cancelFn()
}

func init() {
	jobTaskEntry := &meshInstallJob{
		Data: customData{
			Foo: "some_config_here",
			Bar: 123,
		},
		K8sSvc: NewFakeK8sService(),
	}
	RegisterJobFlow(jobTaskEntry)
}
