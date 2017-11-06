package main

import (
	"errors"
	"fmt"
	//"math/rand"
	"time"

	"github.com/andsha/postgresutils"
	"github.com/andsha/vconfig"
	"github.com/sirupsen/logrus"
)

type upload struct {
	m_sec         vconfig.Section
	m_name        string
	m_vconfig     lvconfig
	m_logger      *logrus.Entry
	m_flags       cmdflags
	m_pgconn      *postgresutils.PostgresProcess
	ping          chan bool
	abortUpload   chan bool
}

type uploadResult struct {
	name   string
	err    error
	logger *logrus.Entry // should be same as upload's logger. Used for logging errors at the end of upload.
}

// Takes copy of config!
func newUpload(sec vconfig.Section,
	vc lvconfig,
	logger *logrus.Logger,
	flags cmdflags,
	pgconn *postgresutils.PostgresProcess) (*upload, error) {

	// attamept to read upload name
	name, err := sec.GetSingleValue("name", "") // name of process
	if err != nil {
		logger.WithFields(logrus.Fields{"section": sec.ToString()}).Error("Cannot resolve name of upload section")
		return nil, errors.New("Cannot resolve name of upload section")
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

    // create table source
    tsName, err := ul.m_sec.GetSingleValue("sourceTable", "")
    if err != nil {res.err = err; result <- *res; return}
    tableSourceSections, err := ul.m_vconfig.GetSectionsByVar("table", "name", tsName)
    if err != nil {res.err = err; result <- *res; return}
    tableSource, err := NewTable(tableSourceSections[0], ul)
    if err != nil {res.err = err; result <- *res; return}

    // get table description
    tsDescription, err := tableSource.getTableDescription()
    if err != nil {res.err = err; result <- *res; return}

    // get time ranges
    tsAvailableDataTimeRanges, err := tableSource.getAvailabeDataTimeRanges()
    if err != nil {res.err = err; result <- *res; return}

    // get destination table
    tdName, err := ul.m_sec.GetSingleValue("destTable", "")
    if err != nil {res.err = err; result <- *res; return}
    tableDestSections, err := ul.m_vconfig.GetSectionsByVar("table", "name", tdName)
    if err != nil {res.err = err; result <- *res; return}
    tableDestination, err := NewTable(tableDestSections[0], ul)
    if err != nil {res.err = err; result <- *res; return}

    // check destination table with source description
    ok, err := tableDestination.checkTable(tsDescription)
    if err != nil {res.err = err; result <- *res; return}
    if !ok {res.err = errors.New(fmt.Sprintf("Error while asseting destination table parameters")); result <- *res; return}

    if len(tsAvailableDataTimeRanges) > 0 {
        // for each time range
        for _, timeRange := range tsAvailableDataTimeRanges {
            // timeRange [startTime, endTime]
            // get new or reset connection to source
            if err := tableSource.connect(); err != nil{res.err = err; result <- *res; return}

            // get new or reset connection to destination
            if err := tableDestination.connect(); err != nil{res.err = err; result <- *res; return}

            // receive data from source
            data, err := tableSource.getData(timeRange)
            if err != nil {res.err = err; result <- *res; return}

            // close source and to cleanup
            if err := tableSource.cleanup(); err != nil{res.err = err; result <- *res; return}

            // change latestUploadedTimeRange in destination to startTime in timeRange
            // upload data to destination
            if err := tableDestination.uploadData(data); err != nil{res.err = err; result <- *res; return}
            // change latestUploadedTimeRange in destination to endTime in timeRange

            // close destination and do cleanup
            if err := tableDestination.cleanup(); err != nil{res.err = err; result <- *res; return}
        }
    } else {
        ul.m_logger.Info("No new data. Skip upload")
    }





















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
