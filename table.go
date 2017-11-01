package main

import (
	"errors"

	"github.com/andsha/postgresutils"

	"github.com/andsha/vconfig"
)

type table interface {
	needRunUpload()
	latestAvailableData()
	getUploadSets()
	copySourceToServer()
	uploadServerToDestination()
}

// ***********************************   Postgres factory **********************
type pgtable struct {
	sec    vconfig.Section
	pgconn *postgresutils.PostgresProcess
}

func (t *pgtable) needRunUpload()             {}
func (t *pgtable) latestAvailableData()       {}
func (t *pgtable) getUploadSets()             {}
func (t *pgtable) copySourceToServer()        {}
func (t *pgtable) uploadServerToDestination() {}

// ***********************************   SFTP factory **********************
type sftptable struct {
	sec    vconfig.Section
	pgconn *postgresutils.PostgresProcess
}

func (t *sftptable) needRunUpload()             {}
func (t *sftptable) latestAvailableData()       {}
func (t *sftptable) getUploadSets()             {}
func (t *sftptable) copySourceToServer()        {}
func (t *sftptable) uploadServerToDestination() {}

// ***********************************   Hadoop factory **********************
type hadooptable struct {
	sec    vconfig.Section
	pgconn *postgresutils.PostgresProcess
}

func (t *hadooptable) needRunUpload()             {}
func (t *hadooptable) latestAvailableData()       {}
func (t *hadooptable) getUploadSets()             {}
func (t *hadooptable) copySourceToServer()        {}
func (t *hadooptable) uploadServerToDestination() {}

// ***********************************   Omniture factory **********************
type omnituretable struct {
	sec    vconfig.Section
	pgconn *postgresutils.PostgresProcess
}

func (t *omnituretable) needRunUpload()             {}
func (t *omnituretable) latestAvailableData()       {}
func (t *omnituretable) getUploadSets()             {}
func (t *omnituretable) copySourceToServer()        {}
func (t *omnituretable) uploadServerToDestination() {}

func NewTable(sec vconfig.Section, historyTableConn *postgresutils.PostgresProcess) (table, error) {
	tType, err := sec.GetSingleValue("type", "")
	if err != nil {
		return nil, err
	}
	switch tType {
	case "postgres":
		t := new(pgtable)
		t.sec = sec
		t.pgconn = historyTableConn
		return t, nil
	case "sftp":
		t := new(sftptable)
		t.sec = sec
		t.pgconn = historyTableConn
		return t, nil
	case "hadoop":
		t := new(hadooptable)
		t.sec = sec
		t.pgconn = historyTableConn
		return t, nil
	case "omniture":
		t := new(omnituretable)
		t.sec = sec
		t.pgconn = historyTableConn
		return t, nil
	default:
		return nil, errors.New("No such type of table")
	}
}

// func NeedRunUpload

// func LatestAvailableData

// func GetUploadSets

// func CopyToServer

// func UploadToDestination
