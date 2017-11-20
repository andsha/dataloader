// Postgres Factory

package main

import(
    "bytes"
    "errors"
    "fmt"
    "os/exec"
    "strconv"
    "strings"
    "time"
    "github.com/andsha/postgresutils"
)

type pgtable struct {
	generictable
    pgprocess *postgresutils.PostgresProcess
}

func (t *pgtable) checkTable(description []tableDescription) error{
    t.upload.m_logger.Debug("description:", description)
    if err := t.connect(); err != nil {return err}
    defer t.disconnect()

    descriptionr := make([]*tableDescription, len(description))
    for idx, _ := range description {
        descriptionr[idx] = &description[idx]
    }

    for _, td := range descriptionr {
        if td.destName == "" {
            td.destName = td.sourceName
        }
        if td.destType == "" {
            td.destType = td.sourceType
        }
    }

    t.upload.m_logger.Debug(description)

    destTableName, err := t.upload.m_sec.GetSingleValue("destTable", "")
    if err != nil {return err}

    destTables, _ := t.upload.m_vconfig.GetSectionsByVar("table", "name", destTableName)
    schema, _ := destTables[0].GetSingleValue("schema", "")
    if err != nil {return err}
    table, _ := destTables[0].GetSingleValue("table", "")
    if err != nil {return err}

    // check if dest table exists in destination
    sql := fmt.Sprintf(`SELECT 1 FROM information_schema.tables
           WHERE table_schema = '%v' AND table_name = '%v'
    `, schema, table)
    t.upload.m_logger.Debug(sql)
    res, err := t.pgprocess.Run(sql)
    if err != nil {return err}

    if len(res) == 0 { // if table does not exist in destination; create it
        fields := ""
        pkey := ""
        for _, td := range descriptionr {
            fields = fmt.Sprintf(`%v, "%v" %v`, fields, td.destName, td.destType)
            if td.isPKey {
                pkey = fmt.Sprintf("%v, %v", pkey, td.destName)
            }
        }

        if pkey != "" {
            pkey = fmt.Sprintf(", primary key (%v)", pkey[1:])
        }
        t.upload.m_logger.Debug("fields:", fields)
        sql := fmt.Sprintf(`CREATE TABLE "%v"."%v" (%v%v)
        `, schema, table, fields[1:], pkey)
        t.upload.m_logger.Debug()

        if _, err := t.pgprocess.Run(sql); err != nil {return err}
        return nil
    }

    // if table does exists in destination, check it
    // List of fields in destination table
    sql = fmt.Sprintf(`SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
                        FROM information_schema.columns
                        WHERE table_schema = '%v' and table_name = '%v'
                        `, schema, table)

    res, err = t.pgprocess.Run(sql)
    if err != nil {return err}

    for _, td := range descriptionr {
        found := false
        for _, col := range res {
            colName, _ := col[0].(string)
            // if col exists in destination
            if colName == td.destName {
                found = true
                colType, _ := col[1].(string)
                // if column type not same as in destination
                if colType == td.destType {
                    continue
                }
                ctype := td.destType
                if strings.HasPrefix(td.destType, "character varying") && colType == "character varying" {
                    charLength, _ := strconv.Atoi(td.destType[8:len(td.destType)-2])
                    colCharLengths, _ := col[2].(string)
                    colCharLength, _ := strconv.Atoi(colCharLengths)
                    if charLength > colCharLength {
                        ctype = fmt.Sprintf("character varying(%v)", charLength)
                    }
                }
                if strings.HasPrefix(td.destType, "numeric") && colType == "numeric" {
                    nps := td.destType[9:len(td.destType)-2]
                    np, _ := col[3].(int)
                    ns, _ := col[4].(int)
                    if nps != fmt.Sprintf("%v, %v", np, ns) {
                        ctype = td.destType
                    }

                }
                sql := fmt.Sprintf(`ALTER TABLE "%v"."%v"
                                    ALTER COLUMN "%v" TYPE %v
                                    `, schema, table, colName, ctype)
                if _, err := t.pgprocess.Run(sql); err != nil {return err}
            }
        }
        // if not found in destination - create new column
        if !found {
            sql := fmt.Sprintf(`ALTER TABLE "%v"."%v" ADD COLUMN "%v" %v`, schema, table, td.destName, td.destType)
            if _, err := t.pgprocess.Run(sql); err != nil {return err}
        }
    }

    // Check Primary keys

    sql = fmt.Sprintf(`
        SELECT kc.column_name
        FROM information_schema.key_column_usage kc
        WHERE
            kc.constraint_name like '%v'
            and kc.table_schema = '%v'
            and  kc.table_name = '%v'
        `, "%_pkey", schema, table)
    res, err = t.pgprocess.Run(sql)
    if err != nil {return err}

    for _, td := range descriptionr {
        exists := false

        for _, pkey := range res{
            pkname, _ := pkey[0].(string)
            t.upload.m_logger.Debug(pkname, td.destName, td.isPKey, td.isPKey && td.destName == pkname)
            if !td.isPKey {exists = true} else {
                if td.destName == pkname {exists = true}
            }

        }
        //t.upload.m_logger.Debug(exists)
        if !exists {
            t.upload.m_logger.Debug("DROPPING PRIMARY KEY")
            // get name of primary key
            sql := fmt.Sprintf(`SELECT constraint_name
                                FROM information_schema.key_column_usage
                                WHERE constraint_name like '%v' and
                                table_schema = '%v' and
                                table_name = '%v'
                                `, "%_pkey", schema, table)
            res, err := t.pgprocess.Run(sql)
            if err != nil {return err}

            if len(res) > 0 {
                pkeyName, _ := res[0][0].(string)
                // delete primery key
                sql = fmt.Sprintf(`ALTER TABLE "%v"."%v"
                                   DROP CONSTRAINT "%v"
                                   `, schema, table, pkeyName)
                if _, err := t.pgprocess.Run(sql); err != nil {return err}
            }

            newpk := ""

            for _, td1 := range descriptionr {
                t.upload.m_logger.Debug(td1)
                if td1.isPKey {
                    newpk = fmt.Sprintf(`%v, "%v"`,  newpk, td1.destName)
                }
            }
            sql = fmt.Sprintf(`ALTER TABLE "%v"."%v"
                               ADD PRIMARY KEY (%v)
                               `, schema, table, newpk[1:])
            if _, err := t.pgprocess.Run(sql); err != nil {return err}
            return nil
        }
    }

    return nil
}

func (t *pgtable) getTableDescription() ([]tableDescription, error){
    if err := t.connect(); err != nil {return nil, err}
    defer t.disconnect()

    var description []tableDescription
    var err error
    description, err = t.getGenericTableDescription()
    if err != nil {return nil, err}

    if description != nil {return description, nil}
    sourceTableName, err := t.upload.m_sec.GetSingleValue("sourceTable", "")
    if err != nil {return nil, err}

    sourceTables, _ := t.upload.m_vconfig.GetSectionsByVar("table", "name", sourceTableName)
    schema, err := sourceTables[0].GetSingleValue("schema", "")
    if err != nil {return nil, err}
    table, err := sourceTables[0].GetSingleValue("table", "")
    if err != nil {return nil, err}

    //check if table exists in source
    sql := fmt.Sprintf(`SELECT 1 FROM information_schema.tables
           WHERE table_schema = '%v' AND table_name = '%v'
    `, schema, table)
    res, err := t.pgprocess.Run(sql)
    if err != nil {return nil, err}
    if len(res) == 0 {
        return nil, errors.New(fmt.Sprintf("Table %v.%v does not exist in source", schema, table))
    }

    // list of excluded fields
    var excludedFields []string
    efs, err := t.tablesection.GetSingleValue("excludedFields", "")
    if err != nil {return nil, err}

    if efs != "" {
        for _, ef := range strings.Split(efs, ",") {
            excludedField := strings.Trim(ef, " ")
            excludedFields = append(excludedFields, excludedField)
        }
    }

    // primary keys
    pkeys := []string{}
    pks, err := t.tablesection.GetSingleValue("primary_key", "")
    if err != nil {return nil, err}
    if pks != "" {
        for _, pk := range strings.Split(pks, ",") {
            primary_key := strings.Trim(pk, " ")
            pkeys = append(pkeys, primary_key)
        }
        t.upload.m_logger.Debug(pkeys)
    } else {
        // Get list of primary keys
        sql := fmt.Sprintf(`
            SELECT kc.column_name
            FROM information_schema.key_column_usage kc
            WHERE
                kc.constraint_name like '%v'
                and kc.table_schema = '%v'
                and  kc.table_name = '%v'
        `, "%_pkey", schema, table)
        t.upload.m_logger.Debug(sql)
        res, err := t.pgprocess.Run(sql)
        if err != nil {return nil, err}

        for _, pkey := range res {
           pk, ok := pkey[0].(string)
            if !ok {return nil, errors.New(fmt.Sprintf("Cannot exctract primary key field name '%v' from table", pkey))}
            pkeys = append(pkeys, pk)
        }
    }

    // List of fields in source table
    sql = fmt.Sprintf(`SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale
                        FROM information_schema.columns
                        WHERE table_schema = '%v' and table_name = '%v'
    `, schema, table)
    res, err = t.pgprocess.Run(sql)
    t.upload.m_logger.Debug(sql, "\n", res)
    if err != nil {return nil, err}

    for _, fieldline := range res {
        var td tableDescription
        t.upload.m_logger.Debug(fieldline)
        s, ok := fieldline[0].(string)
        if !ok {return nil, errors.New(fmt.Sprintf("Could no extract field name '%v' from table", fieldline[0]))}

        // if in excluded field then do not include
        donotinclude := false
        for _, ef := range excludedFields {
            if ef == s {donotinclude = true}
        }
        if donotinclude {continue}
        td.sourceName = s
        s, ok = fieldline[1].(string)
        if !ok {return nil, errors.New(fmt.Sprintf("Could no extract field type '%v' from table", fieldline[1]))}
        td.sourceType = s

        if td.sourceType == "character varying" {
            s, ok := fieldline[2].(int64)
            if !ok {return nil, errors.New(fmt.Sprintf("Could no extract varchar length '%v' from table", fieldline[2]))}
            td.sourceType = fmt.Sprintf("character varying(%v)", s)
        }

        if td.sourceType == "numeric" {
            np, ok := fieldline[3].(int64)
            if !ok {return nil, errors.New(fmt.Sprintf("Could no extract numeric precision '%v' from table", fieldline[3]))}
            ns, ok := fieldline[4].(int64)
            if !ok {return nil, errors.New(fmt.Sprintf("Could no extract numeric precision '%v' from table", fieldline[4]))}
            td.sourceType = fmt.Sprintf("numeric(%v, %v)", np, ns)
        }

        for _, pkey := range pkeys {
            if pkey == td.sourceName {
                td.isPKey = true
            }
        }
        description = append(description,td)
    }

    // MD5
    md5s, err := t.tablesection.GetSingleValue("addMD5ToFields", "")
    if err != nil {return nil, err}

    if md5s != "" {
        for _, mdf := range strings.Split(md5s, ",") {
            md5 := strings.Trim(mdf, " ")
            for _, td := range description {
                if md5 == td.destName {
                    var newtd tableDescription
                    newtd.sourceName = fmt.Sprintf(`md5("%v")`, md5)
                    newtd.destName = fmt.Sprintf(`md5(%v)`, toLowerAndNoPrefix(td.destName))
                    newtd.md5 = true
                    newtd.sourceType = "text"
                    description = append(description, newtd)
                }
            }
        }
    }
    t.upload.m_logger.Debug("Description:", description)
    return description, nil
}

// get time ranges for available data in source.
// will only be used for source
func (t *pgtable) getAvailabeDataTimeRanges() ([][]time.Time, error) {
    if err := t.connect(); err != nil {return nil, err}
    defer t.disconnect()
    // name for timeRanges table
    destTableName, err := t.upload.m_sec.GetSingleValue("destTable", "")
    if err != nil {return nil, err}

    // get schema name for timeRanges table
    pgConnWiteReportsSection, _ := t.upload.m_vconfig.GetSections("writeReportsPGDB")
    pgConnWiteReportsSchema, err := pgConnWiteReportsSection[0].GetSingleValue("schema", "")
    if err != nil {return nil, err}

    //  tablename
    tablename, err := t.tablesection.GetSingleValue("name", "")
    if err != nil {return nil, err}

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
    host, err := t.tablesection.GetSingleValue("host", "")
    if err != nil {return nil, err}

    hostSections, _ := t.upload.m_vconfig.GetSectionsByVar("host", "name", host)
    latsql, err := hostSections[0].GetSingleValue("sqlLatestAvailableTime", "")
    if err != nil {return nil, err}

    if len(latsql) > 0 {
        if res, err := t.pgprocess.Run(latsql); err != nil {return nil, err} else{
            if tm, ok := res[0][0].(time.Time); ok {
                latestAvailableTime = tm
            }else {return nil, errors.New(fmt.Sprintf("Error during getting latest available time for table %v ", tablename))}
        }
    }

    // if sqlLatestAvailableTime is in table section
    latsql, err = t.tablesection.GetSingleValue("sqlLatestAvailableTime", "")
    if err != nil {return nil, err}

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
    sdays, err := t.upload.m_sec.GetSingleValue("daysToLoad", "")
    if err != nil {return nil, err}

    if len(sdays) > 0 {d, _ := strconv.Atoi(sdays); days = d}

     // define force
    force := t.upload.m_flags.force

    // define full reload
    fullReload := false
    fr, err := t.upload.m_sec.GetSingleValue("fullReload", "")
    if err != nil {return nil, err}

    if len(fr) > 0{
        fullReload = true
    }

    // define perMonth
    perMonth := false
    pm, err := t.upload.m_sec.GetSingleValue("perMonth", "false")
    if err != nil {return nil, err}
    if len(pm) > 0{
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

         t.upload.m_logger.Debug("lad", latestAvailableTime, "toDate", toDate, "uploadTime", uploadTime)
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
            t.upload.m_logger.Debug(uploadTime, latestAvailableTime, uploadTime.Before(latestAvailableTime))
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
        t.upload.m_logger.Debug(start, end)
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

func (t *pgtable) connect() error {
    // disconnect
    if err := t.disconnect(); err != nil {return err}

    //reconnect
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
    output, errout, err := executeBashCmd("sleep 1", t.upload.abortUpload)
    t.upload.m_logger.Debug("output:'", output, "' output error:'", errout, "' error:'", err, "'")
    if err != nil {
        return nil, err
    }

    return nil, nil
}

// TODO - why in pgtable???
// executes command in bash. Keeps track of process and kills it if it becomes terminated
// returns standard output, standard error, and result. result is <nil> if no error
func executeBashCmd(command string, abort <-chan bool) (string, string, error) {
    if abort == nil {
        abort = make(chan bool)
    }
    // create command
    cmd := exec.Command("sh", "-c", command)

    //get stdout to read result
    stdout, err := cmd.StdoutPipe()
    if err != nil {
        return "", "", err
    }

    // get error output
    stderr, err := cmd.StderrPipe()
    if err != nil {
        return "", "", err
    }

    // start command
    if err := cmd.Start(); err != nil {
        return "", "", err
    }

    // get pid
    pid := cmd.Process.Pid

    stop := make(chan bool, 1)
    // check that executing process is still alive. kill if terminated
    go func(){
        for {
            pingcmd := fmt.Sprintf("ps -p %v -o stat | grep -v STAT", pid)
            ping, _ := exec.Command("sh", "-c", pingcmd).Output()
            if string(ping[0:1]) == "T" {
                cmd.Process.Kill()
                return
            }
            select {
                case <-abort :
                    fmt.Println("kill me")
                    cmd.Process.Kill()
                    return
                case <-stop:
                    return
                default:
            }
            time.Sleep(time.Second * 10)
        }
    }()

    // read any standard output into buffer
    output := new(bytes.Buffer)
    output.ReadFrom(stdout)

    // read any error output into buffer
    errout := new(bytes.Buffer)
    errout.ReadFrom(stderr)

    //Wait for command to finish
    res := cmd.Wait()

    stop <- true
    // return result
    return output.String(), errout.String(), res
}



func (t * pgtable) updateTimeRanges (start time.Time, end time.Time, beforeUpload bool) error {
    // name for timeRanges table
    destTableName, err := t.upload.m_sec.GetSingleValue("destTable", "")
    if err != nil {return  err}

    // change latestUploadedTimeRange in destination to startTime in timeRange
    pgConnWiteReportsSection, _ := t.upload.m_vconfig.GetSections("writeReportsPGDB")
    pgConnWiteReportsSchema, err := pgConnWiteReportsSection[0].GetSingleValue("schema", "")
    if err != nil {return  err}

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

