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
	m_pgconTimeRanges      *postgresutils.PostgresProcess
	ping          chan bool
	abortUpload   chan bool
    m_sourceTable *table
    m_destTable   *table
}

type uploadResult struct {
	name   string
	err    error
	logger *logrus.Entry // should be same as upload's logger. Used for logging errors at the end of upload.
}

type tableDescription struct {
    sourceName string
    destName string
    sourceType string
    destType string
    isPKey bool
    md5 bool
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
	ul.m_pgconTimeRanges = pgconn

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
	stop := make(chan bool, 1)

	// pinging routine
	go ul.pingUpload(ul.ping, stop)

	ul.m_logger.Info("Start upload")

    // get source table
    tsName, err := ul.m_sec.GetSingleValue("sourceTable", "")
    if err != nil {res.err = err; result <- *res; return}
    tableSourceSections, err := ul.m_vconfig.GetSectionsByVar("table", "name", tsName)
    if err != nil {res.err = err; result <- *res; return}
    tableSource, err := NewTable(tableSourceSections[0], ul)
    if err != nil {res.err = err; result <- *res; return}
    ul.m_sourceTable = &tableSource
    ul.m_logger.Debug("source table created")

    // get table description
    ul.m_logger.Debug("Start generating table description")
    tsDescription, err := tableSource.getTableDescription()
    if e := ul.checkError(err, res, result, stop); e != nil {return}
    ul.m_logger.Debug("Table description generated")

     // get time ranges
    tsAvailableDataTimeRanges, err := tableSource.getAvailabeDataTimeRanges()
    if e := ul.checkError(err, res, result, stop); e != nil {return}
    ul.m_logger.Debug("time ranges generated", tsAvailableDataTimeRanges)

    // get destination table
    tdName, err := ul.m_sec.GetSingleValue("destTable", "")
    if e := ul.checkError(err, res, result, stop); e != nil {return}
    tableDestSections, err := ul.m_vconfig.GetSectionsByVar("table", "name", tdName)
    if e := ul.checkError(err, res, result, stop); e != nil {return}
    tableDestination, err := NewTable(tableDestSections[0], ul)
    if e := ul.checkError(err, res, result, stop); e != nil {return}
    ul.m_destTable = &tableDestination
    ul.m_logger.Debug("destination table created")

    // check destination table with source description
    ul.m_logger.Debug("Begin checing table")
    err = tableDestination.checkTable(tsDescription)
    if e := ul.checkError(err, res, result, stop); e != nil {return}
    ul.m_logger.Debug("Destination table checked")

    if len(tsAvailableDataTimeRanges) > 0 {
        // for each time range
        for _, timeRange := range tsAvailableDataTimeRanges {
            // check if main process is still running
            select {
                case <-ul.abortUpload:
                    ul.m_logger.Info("Upload received kill signal from main process. Cleaning up.")
                    ul.cleanup()
                    ul.m_logger.Info("Exiting")
                    stop <- true
                    return
                default:
            }

            // timeRange [startTime, endTime]
            start := timeRange[0]
            end := timeRange[1]
            ul.m_logger.Info(fmt.Sprintf("Uploading for [%v, %v] time range", start, end))

            // get new or reset connection to source
            err := tableSource.connect()
            if e := ul.checkError(err, res, result, stop); e != nil {return}

            // get new or reset connection to destination
            err = tableDestination.connect()
            if e := ul.checkError(err, res, result, stop); e != nil {return}

            // receive data from source
            ul.m_logger.Debug("Start getting data from source")
            data, err := tableSource.getData(timeRange)
            if e := ul.checkError(err, res, result, stop); e != nil {return}
            ul.m_logger.Debug("Received data from source")

            // Update time ranges table
            err = tableSource.updateTimeRanges(start, end, true)
            if e := ul.checkError(err, res, result, stop); e != nil {return}

            // upload data to destination
            err = tableDestination.uploadData(data)
            if e := ul.checkError(err, res, result, stop); e != nil {return}

            // change latestUploadedTimeRange in destination to endTime in timeRange
            err = tableSource.updateTimeRanges(start, end, false)
            if e := ul.checkError(err, res, result, stop); e != nil {return}

            ul.m_logger.Info(fmt.Sprintf("Uploading for [%v, %v] time range finished", start, end))

        }
    } else {
        ul.m_logger.Info("No new data. Skip upload")
    }

    // close source and destination and do cleanup
    ul.cleanup()

	// once finished return result via channel
	result <- *res
}

// cleaning up upload
func (ul *upload) cleanup() {
    // cleaning up source
    if ul.m_sourceTable != nil {
        if err := (*ul.m_sourceTable).cleanup(); err != nil {
            ul.m_logger.Error("Error during cleanin up source table", err)
        }
    }
    // cleaning up destination
    if ul.m_destTable != nil {
        if err := (*ul.m_destTable).cleanup(); err != nil {
            ul.m_logger.Error("Error during cleanin up destination table", err)
        }
    }
}

// checking errors
func (ul *upload) checkError(err error, res *uploadResult, result chan<- uploadResult, stop chan bool) error {
    if err != nil {
        ul.m_logger.Info("Error in upload", err)
        ul.cleanup() // do clean up
        res.err = err // write error to result struct
        result <- *res // return result channel to main process
        stop <- true // sent stop to pingUpload
        return err // return err to upload
    }
    return nil
}

func (ul *upload) pingUpload(ping <-chan bool, stop <-chan bool) {
	for {
		select {
		case <-ping:
			// do some checking-up on the running upload process;
			// if everything is ok log process is still running
            //TODO add info how long process is running
			ul.m_logger.Info("Process running fine")
		// stop this goroutine
		case <-stop:
			return
		default:
			time.Sleep(time.Second)
		}
	}
}
