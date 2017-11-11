// Postgres Factory

package main

import(
    "errors"
    "fmt"
    "time"
    "strconv"
    "strings"
    "github.com/andsha/postgresutils"
)

type pgtable struct {
	generictable
    pgprocess *postgresutils.PostgresProcess
}



func (t *pgtable) getTableDescription() ([]tableDescription, error){
    var description []tableDescription
    var err error
    description, err = t.getGenericTableDescription()
    if err != nil {return nil, err}
    if description != nil {return description, nil}
    sourceTableName, _ := t.upload.m_sec.GetSingleValue("sourceTable", "")
    sourceTables, _ := t.upload.m_vconfig.GetSectionsByVar("table", "name", sourceTableName)
    schema, _ := sourceTables[0].GetSingleValue("schema", "")
    table, _ := sourceTables[0].GetSingleValue("name", "")

    // list of excluded fields
    var excludedFields []string
    efs, _ := t.tablesection.GetSingleValue("excludedFields", "")
    if efs != "" {
        for _, ef := range strings.Split(efs, ",") {
            excludedField := strings.Trim(ef, " ")
            excludedFields = append(excludedFields, excludedField)
        }
    }

    // List of fields in source table
    sql := fmt.Sprintf(`SELECT column_name, data_type, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_schema = '%v' and table_name = '%v'
    `, schema, table)
    res, err := t.pgprocess.Run(sql)
    if err != nil {return nil, err}

    for _, fieldline := range res {
        var td tableDescription
        s, ok := fieldline[0].(string)
        if !ok {return nil, errors.New(fmt.Sprintf("Could no extract field name '%v' from table", fieldline[0]))}

        // if in excluded field then do not include
        for _, ef := range excludedFields {
            if ef == s {continue}
        }
        td.sourceName = s
        s, ok = fieldline[1].(string)
        if !ok {return nil, errors.New(fmt.Sprintf("Could no extract field name '%v' from table", fieldline[1]))}
        td.sourceType = s

        if td.sourceType == "varchar" {
            s, ok = fieldline[1].(string)
            if !ok {return nil, errors.New(fmt.Sprintf("Could no extract field name '%v' from table", fieldline[2]))}
            td.sourceType = fmt.Sprintf("varchar(%v)", s)
        }
        description = append(description,td)
    }

    // Get list of primary keys
    sql = fmt.Sprintf(`
        SELECT kc.column_name
        FROM information_schema.key_column_usage kc
        WHERE
            kc.constraint_name like '%v'
            and kc.table_schema = '%v'
            and  kc.table_name = '%v'
    `, "%_pkey", schema, table)
    res, err = t.pgprocess.Run(sql)
    if err != nil {return nil, err}

    for _, pkey := range res[0] {
        for _, td := range description {
            pk, ok := pkey.(string)
            if !ok {return nil, errors.New(fmt.Sprintf("Cannot exctract primary key field name '%v' from table", pkey))}
            if pk == td.sourceName {
                td.isPKey = true
            }
        }
    }

    // MD5
    if md5s, _ := t.tablesection.GetSingleValue("addMD5ToFields", ""); md5s != "" {
        for _, mdf := range strings.Split(md5s, ",") {
            md5 := strings.Trim(mdf, " ")
            for _, td := range description {
                if md5 == td.destName {
                    td.md5 = true
                }
            }
        }
    }




    return description, nil
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

    // find latest upload time (toDate) intimeRanges table
    sql := fmt.Sprintf(`SELECT "ToDate"
            FROM "%v"."destTableTimeRanges"
            WHERE "destinationTable" = '%v' `, pgConnWiteReportsSchema, destTableName)
    res, err := t.upload.m_pgconTimeRanges.Run(sql)
    if err != nil {return nil, err}
    if len(res) > 1 {

        return nil, errors.New(fmt.Sprintf("There is more than one entry for table %v in time ranges table", tablename))
    }
    uploadTime := time.Date(2007, time.January, 01, 0, 0, 0, 0, time.UTC)
    if len(res) != 0{
        ut := res[0][0]
        ok := false
        uploadTime, ok = ut.(time.Time)
        if !ok {return nil, errors.New(fmt.Sprintf("Error during getting lupload time for table %v ", tablename))}
    }
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
        toDate, err = time.Parse("2006-01-02 15:04:05", td)
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
    if pm, _ := t.upload.m_sec.GetSingleValue("perMonth", "false"); len(pm) > 0{
        if strings.ToLower(pm) == "true"{
            perMonth = true
        }
    }

    timeRanges := make([][]time.Time, 0)

    // if fullReload
    if fullReload {
        if uploadTime.Before(beginning) {return nil, errors.New(fmt.Sprintf("ToDate from timeRanges table is before beginning for table '%v' ", tablename))}
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
            if !uploadTime.Before(latestAvailableTime) {
                //t := var []time.Time{}
                return [][]time.Time{}, nil
            }
            if beginning.After(uploadTime) {
                start = beginning.AddDate(0,0,-days)
            } else {
                start = uploadTime.AddDate(0,0,-days)
            }
        }

        if start.After(end) {return nil, errors.New(fmt.Sprintf("Start date after End date for table %v", tablename))}

        // make sure start is before ToDate in time ranges
        if uploadTime.Before(start) {return nil, errors.New(fmt.Sprintf("ToDate (%v) from timeRanges is before start time (%v) for table '%v' ", uploadTime, start, tablename))}

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

func (t *pgtable) checkTable(descrition []tableDescription) (bool, error){
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
    if t.pgprocess != nil{
        return t.pgprocess.CloseDB()
    } else {
        return nil
    }

}

func (t *pgtable) cleanup() error {
    if err := t.disconnect(); err != nil {return err}
    // delete t.tempFiles
    t = nil
    return nil
}

func (t *pgtable) getData([]time.Time) ([]byte, error) {
    return nil, nil
}

func (t * pgtable) updateTimeRanges (start time.Time, end time.Time, beforeUpload bool) error {
    // name for timeRanges table
    destTableName, _ := t.upload.m_sec.GetSingleValue("destTable", "")

    // change latestUploadedTimeRange in destination to startTime in timeRange
    pgConnWiteReportsSection, _ := t.upload.m_vconfig.GetSections("writeReportsPGDB")
    pgConnWiteReportsSchema, _ := pgConnWiteReportsSection[0].GetSingleValue("schema", "")
    sql := ""
    t.upload.m_logger.Debug("beforeUpload:", beforeUpload)
    if beforeUpload{
        tm := start.Format("2006-01-02 15:04:05")
        sql = fmt.Sprintf(`UPDATE "%v"."destTableTimeRanges"
                            SET "ToDate" = '%v'
                            WHERE "destinationTable" = '%v'
        `, pgConnWiteReportsSchema, tm, destTableName)
    } else {
        sql = fmt.Sprintf(`SELECT 1 FROM "%v"."destTableTimeRanges"
                          WHERE "destinationTable" = '%v'
                        `, pgConnWiteReportsSchema, destTableName)
        res, err := t.upload.m_pgconTimeRanges.Run(sql)
        if err != nil {return err}
        //t.upload.m_logger.Debug("wwwwwwwwwwwwww", res[0][0])

        if len(res) > 0 {
            tm := end.Format("2006-01-02 15:04:05")
            sql = fmt.Sprintf(`UPDATE "%v"."destTableTimeRanges"
                                SET "ToDate" = '%v'
                                WHERE "destinationTable" = '%v'
            `, pgConnWiteReportsSchema, tm, destTableName)
        } else {
            tstart := start.Format("2006-01-02 15:04:05")
            tend := end.Format("2006-01-02 15:04:05")
            timenow := time.Now().Format("2006-01-02 15:04:05")
            sql = fmt.Sprintf(`INSERT INTO "%v"."destTableTimeRanges"
                                ("destinationTable",
                                 "lastModifiedTime",
                                 "FromDate",
                                 "ToDate") VALUES
                                ('%v', '%v', '%v', '%v')
            `, pgConnWiteReportsSchema, destTableName, timenow, tstart, tend)
        }


    }
    t.upload.m_logger.Debug(sql)
    if _, err := t.upload.m_pgconTimeRanges.Run(sql); err != nil {return err}

    return nil

}

func (t *pgtable) uploadData([]byte) error {
    return nil
}

