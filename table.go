package main

import (
	"errors"
    "fmt"
    "time"

	"github.com/andsha/vconfig"
)

type table interface {
    getTableDescription() ([]string, error)
    getAvailabeDataTimeRanges() ([][]time.Time, error)
    checkTable(descrition []string) (bool, error)
    connect() error
    cleanup() error
    disconnect() error
    getData([]time.Time) ([]byte, error)
    updateTimeRanges(time.Time, time.Time, bool) error
    uploadData([]byte) error
}

type generictable struct {
    tablesection    *vconfig.Section
    upload      *upload
    tempFiles []string
}

func NewTable(tsec *vconfig.Section, ul *upload) (table, error) {
	tHostName, err := tsec.GetSingleValue("host", "")
	if err != nil {return nil, err}
    tHostSections, err := ul.m_vconfig.GetSectionsByVar("host", "name", tHostName)
    if err != nil {return nil, err}
    tType, err := tHostSections[0].GetSingleValue("type", "")
    if err != nil {return nil, err}
    ct := new(generictable)
    ct.tablesection = tsec
    ct.upload = ul

    switch tType {
	case "postgres":
		t := new(pgtable)
        t.generictable = *ct
		return t, nil
	default:
		return nil, errors.New(fmt.Sprintf("No such type of table %v", tType))
	}
}
