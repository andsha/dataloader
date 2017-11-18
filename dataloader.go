package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/andsha/postgresutils"
	"github.com/andsha/vconfig"
	"github.com/sirupsen/logrus"
)

type cmdflags struct {
	verbose bool
	config  string
	force   bool
	days    int
	threads int
	todate   string
}

// new instance of logger

func (flags *cmdflags) parseCmdFlags() {
	flag.BoolVar(&flags.verbose, "verbose", false, "Show more output")
	flag.StringVar(&flags.config, "config", "", "path to the file that describes configuration")
	flag.BoolVar(&flags.force, "force", false, "force report running")
	flag.IntVar(&flags.days, "days", 1, "How many days to sync...")
	//TODO change variable threads to instances or something else
    flag.IntVar(&flags.threads, "threads", 4, "How many threads to run...")
	flag.StringVar(&flags.todate, "todate", "", "generate feed for 'todate'")
	flag.Parse()
}

func main() {

	// Get command line flags
	flags := new(cmdflags)
	flags.parseCmdFlags()
	//fmt.Println(flags)
	//Initialize logging
    //TODO log file get from command line
    //TODO do not use log.Fatal, because after script done we have to send email with errors
	var logging = logrus.New()
	logfile, err := os.OpenFile("/tmp/log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {panic(err)}
	defer logfile.Close()
	logging.Out = logfile
	// set default level
    if flags.verbose {
        logging.SetLevel(logrus.DebugLevel)
    } else {
         logging.SetLevel(logrus.InfoLevel)
    }

	logging.Info("Start")

	// Iniialize Errors

	// Read Config
	configFile := flags.config
	cfg, err := vconfig.New(configFile)
	if err != nil {
		logging.Fatal(fmt.Sprintf("Could not read config %v", configFile))
	}
	var config lvconfig // this is extension for vconfig
	config.VConfig = cfg

	// Connect to postgres
	writeReportsPGDBSections, err := config.GetSections("writeReportsPGDB")
	if err != nil {
		logging.Fatal(err)
	}
	var lsec lSection
	lsec.Section = *writeReportsPGDBSections[0]
	dbPrams, err := lsec.getDBConnParameters()
	if err != nil {
		logging.Fatal(err)
	}
    //TODO section is not with secure password but with info how to decode password
	pwdSection, err := config.GetSections("SECURE PASSWORD")
	if err != nil {
		logging.Fatal(err)
	}
	pgconnWriteReports, err := postgresutils.NewDB(dbPrams["host"],
		dbPrams["port"],
		dbPrams["database"],
		dbPrams["user"],
		dbPrams["password"],
		"disable",
		pwdSection[0])
	if err != nil {
		logging.Fatal(err)
	}
    //TODO check that defer happens if Fatal called
	defer pgconnWriteReports.CloseDB()

	// update config by expansion of dbUploads
	if err := config.updateDBUploads(); err != nil {
		logging.Fatal(err)
	}

    fmt.Println(config.ToString())

	// history table
	schema, err := lsec.GetSingleValue("schema", "")
	if err != nil {
		logging.Fatal(err)
	}

	sql := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %v."destTableTimeRanges"(
            "destinationTable"  varchar(255),
            "lastModifiedTime"  timestamp,
            "FromDate"          timestamp,
            "ToDate"            timestamp
        )
    `, schema)
	_, err = pgconnWriteReports.Run(sql)
	if err != nil {
		logging.Fatal(err)
	}

	usections, err := config.GetSections("upload")
	fmt.Println(len(usections))
	if err != nil {
		logging.Fatal(err)
	}

	uploads := make(map[int][]*upload)
    errorList := make([]string, 0)
    allUploads := 0

	// create queue of uploads according to queue
    //TODO add priority within the queue
	for _, uploadSection := range usections {
		// we copy current config to new upload object, thus all futher modifications
		// won't be propagated to upload
		ul, err := newUpload(*uploadSection, config, logging, *flags, pgconnWriteReports)
		if err != nil {logging.Fatal(err)}
		queue, err := ul.m_sec.GetSingleValue("priority", "5")
		if queue == "" {logging.Fatal(err)}
		nqueue, _ := strconv.Atoi(queue)

		if _, ok := uploads[nqueue]; ok {
			uploads[nqueue] = append(uploads[nqueue], ul)
		} else {
			q := make([]*upload, 1)
			q[0] = ul
			uploads[nqueue] = q
		}
        allUploads++
	}

    //TODO these are not priorities but queue (just to avid confusion)
	priorities := make([]int, 0)

	for p := range uploads {
		priorities = append(priorities, p)
	}
	sort.Ints(priorities)

	runningUploads := 0
	runningUploadSections := make(map[string]*upload)
	var rusMutex = &sync.Mutex{}

	// Channel for receiving upload rsult
	result := make(chan uploadResult, flags.threads)

	// Channel for stopping checkUploadResult routine
	stop := make(chan bool, 1)

	//channel indicating checkUploadResult routine is done
	done := make(chan bool, 1)

    //channel indicating checkUploadResult routine is done
	interrupt := make(chan bool, 1)

	// create ticker for pinging running uploads
	ticker := time.NewTicker(time.Second * 60)

	//start goroutine for checking syscalls to catch kill calls
	kill := make(chan os.Signal, 1)
	signal.Notify(kill, os.Interrupt, syscall.SIGKILL, syscall.SIGINT, syscall.SIGTSTP, syscall.SIGTERM)

	// start goroutine for checking results
	go checkUploadResult(result,
                        &runningUploads,
                        logging,
                        stop,
                        done,
                        interrupt,
                        rusMutex,
                        runningUploadSections,
                        ticker.C,
                        kill,
                        &errorList)

	killdone := false
    // Start runUpload for each upload from priority map. maximum number of
	// routines is defined in command line.
    // TODO priority here is queue

    label_for:
        for _, priority := range priorities {
            // get latest upload waiting in the queue
    		for _, ul := range uploads[priority] {

                    for {
        				// get number of running upload sections
        				rusMutex.Lock()
        				runningUploads = len(runningUploadSections)
        				rusMutex.Unlock()

                        // when more recources available than running jobs
        				select {
                        case <-interrupt:
                                logging.Debug("SELECT")
                                killdone = true
                                break label_for
                        default:
                        }
                        if runningUploads < flags.threads {
                           // create new channel for pinging
        					ping := make(chan bool, 1)
        					ul.ping = ping

                            // create channel for killing upload routines
        					abortUpload := make(chan bool, 1)
        					ul.abortUpload = abortUpload
                            // start goroutine
        					go ul.runUpload(result)

                            // add upload section to slice of running sections
        					rusMutex.Lock()
        					runningUploadSections[ul.m_name] = ul
        					rusMutex.Unlock()
        					break
        				} else {
            					fmt.Println("waiting for thread to become available")
            					time.Sleep(time.Second)
        				}
        			}
    		}
    	}


    // wait until last routines (alive or killed) are done
    logging.Debug("waiting for uploads to finish")
	for {
		rusMutex.Lock()
		runningUploads = len(runningUploadSections)
		rusMutex.Unlock()
		if runningUploads == 0 {
			break
		}
		fmt.Println("waiting to finish", runningUploads)
		time.Sleep(time.Second)
	}
    logging.Debug("all uploads finished")

	// Stop checkUploadResult
	stop <- true

	// wait until checkUploadResult routine is done
	<-done

	if killdone {
        logging.Error("main process was terminated")
    }
    logging.Info(fmt.Sprintf("There was %v errors in %v uploads", len(errorList), allUploads))
    for _, e := range errorList {
        logging.Error(e)
    }

    //TODO where will be sent email with all the errors during run?
}

func checkUploadResult(reschan <-chan uploadResult,
	runningUploads *int,
	logging *logrus.Logger,
	stop <-chan bool,
	done chan<- bool,
    interrupt chan<- bool,
	rusMutex *sync.Mutex,
	runningUploadSections map[string]*upload,
	ticker <-chan time.Time,
	kill <-chan os.Signal,
    errorList *[]string) {
	for {
		select {

        // when upload return result
        case result := <-reschan:
			logging.Debug("receive result from upload")
            if result.err != nil {
                // log error message
                result.logger.Error(result.err)
                // add error to list of errors
                *errorList = append(*errorList, fmt.Sprintf("Upload '%v' encounter error: %v", result.name, result.err))
			}

			// remove upload section from slice of running sections
            // main prorgam should allocate new job for freed thread
			rusMutex.Lock()
			delete(runningUploadSections, result.name)
			rusMutex.Unlock()

        // ask upload for status update
        case <-ticker:
			//rusMutex.Lock()
			for _, ul := range runningUploadSections {
				ul.ping <- true
			}
			//rusMutex.Unlock()

        // watch for syscalls' terminate signal
        case k := <-kill:
			switch k {
    			case syscall.SIGKILL, syscall.SIGINT, syscall.SIGTSTP, syscall.SIGTERM:
    				logging.Debug("kill main process, ", k)
                    // stop main process from creating new uploads
                    interrupt<-true
                    logging.Debug("waiting")
                    // to make sure all runUploads were put to the list of active uploads
                    time.Sleep(time.Second)
                    // kill active uploads
                    logging.Debug("start sending kill signals")
                    for _, ul := range runningUploadSections {
    					// send once to kill upload or runBashCmd
                        logging.Debug("kill it")
                        ul.abortUpload <- true
    				}
                    //rusMutex.Unlock()
			}

        // kill this goroutine
		case <-stop:
            logging.Debug("STOP")
			done <- true
			return

        // otherwise sleep
        default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}
