// Postgres Factory

package main

import(
    "errors"
    "fmt"
    "time"
    "strconv"
    "github.com/andsha/postgresutils"
)

type pgtable struct {
	generictable
    pgprocess *postgresutils.PostgresProcess
}



func (t *pgtable) getTableDescription() ([]string, error){
    return []string{""}, nil
}

// get time ranges for available data in source.
// will only be used for source
func (t *pgtable) getAvailabeDataTimeRanges() ([][]time.Time, error) {
    // name for timeRanges table
    destTableName, _ := t.upload.m_sec.GetSingleValue("destTable", "")

    // get schema name for timeRanges table
    pgConnWiteReportsSection, _ := t.upload.m_vconfig.GetSections("writeReportsPGDB")
    pgConnWiteReportsSchema, _ := pgConnWiteReportsSection[0].GetSingleValue("schema", "")

    //  tablename
    tablename, _ := t.tablesection.GetSingleValue("name", "")

    // find latest upload time intimeRanges table
    sql := fmt.Sprintf(`SELECT "ToDate"
            FROM "%v"."destTableTimeRanges"
            WHERE "destinationTable" = "%v" `, pgConnWiteReportsSchema, destTableName)
    res, err := t.upload.m_pgconn.Run(sql)
    if err != nil {return nil, err}
    if len(res) > 1 {

        return nil, errors.New(fmt.Sprintf("There is more than one entry for table %v in time ranges table", tablename))
    }
    ut := res[0][0]
    uploadTime, ok := ut.(time.Time)
    if !ok {return nil, errors.New(fmt.Sprintf("Error during getting lupload time for table %v ", tablename))}

    // find latest available date
    // default
    latestAvailableTime := time.Now()
    // if sqlLatestAvailableTime is in host section
    host, _ := t.tablesection.GetSingleValue("host", "")
    hostSections, _ := t.upload.m_vconfig.GetSectionsByVar("host", "name", host)
    latsql, _ := hostSections[0].GetSingleValue("sqlLatestAvailableTime", "")
    if len(latsql) > 0 {
        if res, err := t.pgprocess.Run(latsql); err != nil {return nil, err} else{
            if tm, ok := res[0][0].(time.Time); ok {
                latestAvailableTime = tm
            }else {return nil, errors.New(fmt.Sprintf("Error during getting latest available time for table %v ", tablename))}
        }
    }

    // if sqlLatestAvailableTime is in table section
    latsql, _ = t.tablesection.GetSingleValue("sqlLatestAvailableTime", "")
    if len(latsql) > 0 {
        if res, err := t.pgprocess.Run(latsql); err != nil {return nil, err} else{
            if tm, ok := res[0][0].(time.Time); ok {
                latestAvailableTime = tm
            }else {return nil, errors.New(fmt.Sprintf("Error during getting latest available time for table %v ", tablename))}
        }
    }

    // define toDate
    td := t.upload.m_flags.todate
    var toDate time.Time
    if td != "" {
        toDate, err = time.Parse("2017-12-31 14:32:54", td)
    }
    beginning := time.Date(2007, time.January, 01, 0, 0, 0, 0, time.UTC)

    // define days
    days := t.upload.m_flags.days
    sdays, _ := t.upload.m_sec.GetSingleValue("daysToLoad", "")
    if len(sdays) > 0 {d, _ := strconv.Atoi(sdays); days = d}

     // define force
    force := t.upload.m_flags.force

    // define full reload
    fullReload := false
    if fr, _ := t.upload.m_sec.GetSingleValue("fullReload", ""); len(fr) > 0{
        fullReload = true
    }

    // define perMonth
    perMonth := false
    if pm, _ := t.upload.m_sec.GetSingleValue("perMonth", ""); len(pm) > 0{
        perMonth = true
    }

    timeRanges := make([][]time.Time, 0)

    // if fullReload
    if fullReload {
        timeRange := []time.Time {beginning, latestAvailableTime}
        timeRanges = append(timeRanges, timeRange)
    } else {
        var start time.Time
        var end   time.Time

        var niltime time.Time
        if toDate == niltime{
            end = latestAvailableTime
        } else {
            if latestAvailableTime.Before(toDate) {end = latestAvailableTime} else {end = toDate}
        }

        if force {
            start = end.AddDate(0,0,-days)
        } else {
            if beginning.After(uploadTime) {start = beginning.AddDate(0,0,-days)} else {start = uploadTime.AddDate(0,0,-days)}
        }

        if start.After(end) {return nil, errors.New(fmt.Sprintf("Start date after End date for table %v", tablename))}

        if perMonth {
            s := start
            e := start.AddDate(0,1,0)
            for {
                if e.After(end){
                    timeRanges = append(timeRanges, []time.Time {s, end})
                    break
                } else{
                    timeRanges = append(timeRanges, []time.Time {s, e})
                    s = s.AddDate(0,1,0)
                    e = e.AddDate(0,1,0)
                }
            }
        } else {
            timeRange := []time.Time {start, end}
            timeRanges = append(timeRanges, timeRange)
        }
    }

    return timeRanges, nil
}

func (t *pgtable) checkTable(descrition []string) (bool, error){
    return true, nil
}

func (t *pgtable) connect() error {
    host, err := t.tablesection.GetSingleValue("host", "")
    if err != nil {return err}
    pgprocess, err := t.upload.m_vconfig.getPGConn(host)
    if err != nil {return err}
    t.pgprocess = pgprocess
    return nil
}

func (t *pgtable) disconnect() error {
    return t.pgprocess.CloseDB()

}

func (t *pgtable) cleanup() error {
    if err := t.disconnect(); err != nil {return err}
    // delete t.tempFiles
    return nil
}

func (t *pgtable) getData([]time.Time) ([]byte, error) {
    return nil, nil
}

func (t *pgtable) uploadData([]byte) error {
    return nil
}

