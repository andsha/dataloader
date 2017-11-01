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
	today   string
}

// new instance of logger

func (flags *cmdflags) parseCmdFlags() {
	flag.BoolVar(&flags.verbose, "verbose", false, "Show more output")
	flag.StringVar(&flags.config, "config", "", "path to the file that describes configuration")
	flag.BoolVar(&flags.force, "force", false, "force report running")
	flag.IntVar(&flags.days, "days", 7, "How many days to sync...")
	flag.IntVar(&flags.threads, "threads", 5, "How many threads to run...")
	flag.StringVar(&flags.today, "todate", "", "generate feed for 'today'")
	flag.Parse()
}

type lSection struct {
	vconfig.Section
}

func (sec *lSection) getDBConnParameters() (map[string]string, error) {
	dbParams := make(map[string]string)
	host, err := sec.GetSingleValue("host", "")
	if err != nil {
		return nil, err
	}
	dbParams["host"] = host
	port, err := sec.GetSingleValue("port", "5432")
	if err != nil {
		return nil, err
	}
	dbParams["port"] = port
	database, err := sec.GetSingleValue("database", "")
	if err != nil {
		return nil, err
	}
	dbParams["database"] = database
	user, err := sec.GetSingleValue("userName", "")
	if err != nil {
		return nil, err
	}
	dbParams["user"] = user
	password, err := sec.GetSingleValue("password", "5432")
	if err != nil {
		return nil, err
	}
	dbParams["password"] = password

	return dbParams, nil
}

func main() {

	// Get command line flags
	flags := new(cmdflags)
	flags.parseCmdFlags()
	fmt.Println(flags)

	//Initialize logging
	var logging = logrus.New()
	logfile, err := os.OpenFile("/tmp/log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer logfile.Close()
	logging.Out = logfile
	logging.SetLevel(logrus.InfoLevel) // set default level
	logging.Info("Start")

	// Iniialize Errors

	// Read Config
	configFile := flags.config
	config, err := vconfig.New(configFile)
	if err != nil {
		logging.Fatal(fmt.Sprintf("Could not read config %v", configFile))
	}

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
	defer pgconnWriteReports.CloseDB()

	// history table
	schema, err := lsec.GetSingleValue("schema", "")
	if err != nil {
		logging.Fatal(err)
	}

	sql := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %v."destTableTimeRanges"(
            destinationTable  varchar(255),
            lastModifiedTime  timestamp,
            FromDate          timestamp,
            ToDate            timestamp
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
	//numUploads := 0

	// create queue of uploads according to priority
	for _, uploadSection := range usections {
		ul, err := newUpload(*uploadSection, config, logging, *flags, pgconnWriteReports)
		if err != nil {
			logging.Fatal(err)
		}
		queue, err := ul.m_sec.GetSingleValue("priority", "5")
		if queue == "" {
			logging.Fatal(err)
		}
		nqueue, _ := strconv.Atoi(queue)

		if _, ok := uploads[nqueue]; ok {
			uploads[nqueue] = append(uploads[nqueue], ul)
		} else {
			q := make([]*upload, 1)
			q[0] = ul
			uploads[nqueue] = q
		}
	}

	priorities := make([]int, 0)
	for p := range uploads {
		priorities = append(priorities, p)
	}
	sort.Ints(priorities)

	runningUploads := 0
	runningUploadSections := make(map[string]*upload)
	var rusMutex = &sync.Mutex{}

	// Channel to be sent with every goroutine; checks result of upload run
	result := make(chan uploadResult)

	// Channel for stopping checkUploadResult routine
	stop := make(chan bool)

	//channel indicating checkUploadResult routine is done
	done := make(chan bool)

	// create ticker for pinging running uploads
	ticker := time.NewTicker(time.Second * 10)

	//start goroutine for checking syscalls to catch kill calls
	kill := make(chan os.Signal, 3)
	signal.Notify(kill, os.Interrupt, syscall.SIGKILL, syscall.SIGINT, syscall.SIGTSTP, syscall.SIGTERM)

	// start goroutine for checking results
	go checkUploadResult(result, &runningUploads, logging, stop, done, rusMutex, runningUploadSections, ticker.C, kill)

	// Start runUpload for each upload from priority map. maximum number of
	// routines is defined in command line.
	for _, priority := range priorities {
		// get latest upload waiting in the queue
		for _, ul := range uploads[priority] {
			for {
				// get number of running upload sections
				rusMutex.Lock()
				runningUploads = len(runningUploadSections)
				rusMutex.Unlock()
				if runningUploads < flags.threads {
					// create new channel for pinging
					ping := make(chan bool)
					ul.ping = ping
					// channel for killing upload routines
					abortUpload := make(chan bool)
					ul.abortUpload = abortUpload
					// start goroutine
					go ul.runUpload(result)
					// add uploas section to slice of running sections
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

	// wait until last routines are done
	for {
		rusMutex.Lock()
		runningUploads = len(runningUploadSections)
		rusMutex.Unlock()
		if runningUploads == 0 {
			break
		}
		fmt.Println("waiting to finish")
		time.Sleep(time.Second)
	}

	// Stop checkUploadResult
	stop <- true

	// wait until checkUploadResult routine is done
	<-done

	logging.Info("script finished")

}

func checkUploadResult(reschan <-chan uploadResult,
	runningUploads *int,
	logging *logrus.Logger,
	stop <-chan bool,
	done chan<- bool,
	rusMutex *sync.Mutex,
	runningUploadSections map[string]*upload,
	ticker <-chan time.Time,
	kill <-chan os.Signal) {
	for {
		select {
		case result := <-reschan:
			if result.err != nil {
				result.logger.Error(fmt.Sprintf("Upload '%v' encounter error %v", result.name, result.err))
			}
			// remove upload section from slice of running sections
			rusMutex.Lock()
			delete(runningUploadSections, result.name)
			rusMutex.Unlock()
		case <-ticker:
			rusMutex.Lock()
			for _, ul := range runningUploadSections {
				ul.ping <- true
			}
			rusMutex.Unlock()
		case k := <-kill:
			switch k {
			case syscall.SIGKILL, syscall.SIGINT, syscall.SIGTSTP, syscall.SIGTERM:
				for _, ul := range runningUploadSections {
					ul.abortUpload <- true
				}
			}
		case <-stop:
			done <- true
			return
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

/*
syscall.SIGKILL, syscall.SIGINT, syscall.SIGTSTP, syscall.SIGTERM
history table
result: success/not; bool
started: when started; goroutine starts
stop time: when done; goroutine finishes (defer)
name/id from config
parameters: jsonb. until when data was copied;


1. cmd line flags
2. create logging
    create errors
3. create history table (could already exist). in postgres
4. read config file:
    for each 'upload' create upload thread (name as id):
    4.1 create source and destination. for either of them:
        4.1.1 get info about time for latest available data
    4.2 for last run: if not succesful: then run with same parameters as in table
                      else: if time since time in 4.1.1 > from parameters(jsonb) then run routine
                            else: write to log and exit
5. main thread writes into log every minute who is running and how long
6. When all threads are done; exit main program. send email with errors.




*/