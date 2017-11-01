package main

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/andsha/postgresutils"

	"github.com/andsha/vconfig"

	"github.com/sirupsen/logrus"
)

type upload struct {
	m_sec         vconfig.Section
	m_name        string
	m_vconfig     vconfig.VConfig
	m_logger      *logrus.Entry
	m_flags       cmdflags
	m_pgconn      *postgresutils.PostgresProcess
	m_source      *table
	m_destination *table
	ping          chan bool
	abortUpload   chan bool
}

type uploadResult struct {
	name   string
	err    error
	logger *logrus.Entry
}

func newUpload(sec vconfig.Section,
	vc vconfig.VConfig,
	logger *logrus.Logger,
	flags cmdflags,
	pgconn *postgresutils.PostgresProcess) (*upload, error) {

	// attamept to read upload name
	name, err := sec.GetSingleValue("name", "") // name of process
	if err != nil {
		logger.WithFields(logrus.Fields{"section": sec.ToString()}).Error("Cannot resolve name of upload section")
		err := errors.New("Cannot resolve name of upload section")
		return nil, err
	}

	// create new upload object and fill with data
	ul := new(upload)
	ul.m_sec = sec
	ul.m_name = name
	ul.m_vconfig = vc
	ul.m_flags = flags
	ul.m_pgconn = pgconn

	// Modify logger to include name of the process
	logging := logger.WithFields(logrus.Fields{"process": name}) // always write name of process in the log
	logging.Info("Create upload object")
	ul.m_logger = logging

	// Source
	source, err := ul.getTableFromSection("sourceTable")
	if err != nil {
		ul.m_logger.Error(err)
	}
	ul.m_destination = source

	//Destination
	destination, err := ul.getTableFromSection("destTable")
	if err != nil {
		ul.m_logger.Error(err)
	}
	ul.m_destination = destination

	/*
	   1. run upload or not
	      if 1 then:
	   2       source: define sets of uploads. For each set:
	   3       source: copy data from source to file on server
	   4       destinatio: copy file to destination server and upload to destation
	   5 ...


	*/

	return ul, nil
}

func (ul *upload) runUpload(result chan<- uploadResult) {
	res := new(uploadResult)
	res.name = ul.m_name
	res.err = nil
	res.logger = ul.m_logger

	// stopping channel
	stop := make(chan bool)

	// pinging routine
	go ul.pingUpload(ul.ping, ul.abortUpload, stop)

	ul.m_logger.Info("Start upload")

	time.Sleep(time.Second * time.Duration(20+rand.Intn(20)))
	if rand.Intn(10) > 5 {
		res.err = errors.New("me error, la-a-la")
		fmt.Println("Error here")
		result <- *res
		return
	}
	time.Sleep(time.Second * time.Duration(20+rand.Intn(20)))

	ul.m_logger.Info("Finish upload without errors")

	// once finished return result via channel
	result <- *res
}

func (ul *upload) pingUpload(ping <-chan bool, abort <-chan bool, stop <-chan bool) {
	for {
		select {
		case <-ping:
			// do some checking-up on the running upload process;
			// if everything is ok log process is still running
			ul.m_logger.Info("Process running fine")
		case <-abort:
			// abort this upload goroutine
			ul.m_logger.Info("kill this process")
		case <-stop:
			return
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (ul *upload) getTableFromSection(tableType string) (*table, error) {
	tableName, err := ul.m_sec.GetSingleValue(tableType, "")
	if err != nil {
		return nil, err
	}
	sectons, err := ul.m_vconfig.GetSections("table")
	if err != nil {
		return nil, err
	}

	var tablesection *vconfig.Section
	numtables := 0
	for _, tsection := range sectons {
		tname, _ := tsection.GetSingleValue("name", "")
		//fmt.Println(fmt.Sprintf("'%v'", tname), tableName, tname == tableName)
		if tname == tableName {
			tablesection = tsection
			numtables++
		}
	}
	if numtables == 0 {
		return nil, errors.New(fmt.Sprintf("Cannot find table section with name = '%v'", tableName))
	}
	if numtables > 1 {
		return nil, errors.New(fmt.Sprintf("Multiple table sections with name = '%v'.", tableName))
	}

	hostName, err := tablesection.GetSingleValue("host", "")
	if err != nil {
		return nil, err
	}

	hosts, err := ul.m_vconfig.GetSections("host")
	if err != nil {
		return nil, err
	}
	numtables = 0
	var host *vconfig.Section
	for _, thost := range hosts {
		hname, _ := thost.GetSingleValue("name", "")
		//fmt.Println(hname, hostName, hname == hostName)
		if hname == hostName {
			host = thost
			numtables++
		}
	}
	if numtables == 0 {
		return nil, errors.New(fmt.Sprintf("Cannot find host section with name = '%v'", hostName))
	}
	if numtables > 1 {
		return nil, errors.New(fmt.Sprintf("Multiple host sections with name = '%v'.", hostName))
	}

	var tbl table

	tbl, err = NewTable(*host, ul.m_pgconn)
	if err != nil {
		return nil, err
	}

	return &tbl, nil
}
