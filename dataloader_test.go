package main

import (
    "fmt"
    "io/ioutil"
    "os"
    "os/exec"
    "strings"
    "testing"
    "time"
    "errors"

    "github.com/andsha/postgresutils"
)

func TestSingleUpload(t *testing.T) {
    // create folder for Postgres server
    errCount :=0
    defer func(){
        //remove test folder, we should remove everything only if test successfull
        fmt.Println(fmt.Sprintf(" Test is done with %v errors ", errCount))
    }()
    currentDir,  err := os.Getwd()
    if err != nil {t.Fatal(err)}
    fmt.Println(currentDir)

    _ = os.RemoveAll(fmt.Sprintf("%v/test/", currentDir))

    if err := os.MkdirAll(fmt.Sprintf("%v/test/pg", currentDir), 0700); err != nil {t.Fatal(err)}
    defer func(){
        //remove test folder, we should remove everything only if test successfull
        if errCount==0 {
            if err := os.RemoveAll(fmt.Sprintf("%v/test/", currentDir)); err != nil {
                t.Fatal(err)
            }
        }
    }()

    // create postgres server; run initdb
    cmd := fmt.Sprintf("initdb -D %v/test/pg -U testuser", currentDir)
    if err := exec.Command("sh", "-c", cmd).Run(); err != nil {t.Fatal(err)}

    //
    pfile, err := ioutil.ReadFile(fmt.Sprintf("%v/test/pg/postgresql.conf", currentDir))
    if err != nil {t.Fatal(err)}

    lines := strings.Split(string(pfile), "\n")

    for idx, line := range lines {
        if strings.HasPrefix(line, "#port = 5432") {
            lines[idx] = fmt.Sprintf("port = 1196")
        }
    }

    out := strings.Join(lines, "\n")
    if err := ioutil.WriteFile(fmt.Sprintf("%v/test/pg/postgresql.conf", currentDir), []byte(out), 0700); err != nil {t.Fatal(err)}

    // start server
    cmd = fmt.Sprintf("pg_ctl -D %v/test/pg -U testuser start", currentDir)
    if err := exec.Command("sh", "-c", cmd).Run(); err != nil {t.Fatal(err)}
    time.Sleep(time.Second*5)

    defer func(){
        // stop server
        cmd = fmt.Sprintf("pg_ctl -D %v/test/pg -U testuser stop", currentDir)
        if err := exec.Command("sh", "-c", cmd).Run(); err != nil {t.Fatal(err)}
        time.Sleep(time.Second*5)

    }()

    //create db
    fmt.Println("create DB")
    cmd = "createdb -p 1196 testdb -U testuser"
    out, errout, err := executeBashCmd(cmd, nil)
    if err != nil {t.Fatal(err)}
    fmt.Println("out:", out, "errout:",  errout, "err:", err)
    //if err := exec.Command("sh", "-c", cmd).Run(); err != nil {fmt.Println(err);t.Fatalf("%v", err)}

    // connect to postgres
    pgconn, err := postgresutils.NewDB("127.0.0.1", "1196", "testdb", "testuser", "", "disable", nil)
    if err != nil {t.Fatal(err)}
    defer pgconn.CloseDB()

    // create source and destination schemas
    sql := "CREATE SCHEMA source"
    _, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}
    sql = "CREATE SCHEMA destination"
    _, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    // in source schema create tables
    sql = "CREATE TABLE source.table1 (idx int, text varchar(20), date timestamp, if bool, primary key (idx, text))"
    _, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    sql = "CREATE TABLE source.table2 (idx1 int, idx2 int, idx3 int, idx4 int, primary key (idx1, idx2, idx3))"
    _, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    // create schema reportspg
    sql = "CREATE SCHEMA data_schema"
    _, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    // create config
    cfg := `
    [writeReportsPGDB]
    host = 127.0.0.1
    port = 1196
    userName = testuser
    database = testdb
    schema = data_schema

    [host]
    type = postgres
    name = testhost
    host = 127.0.0.1
    port = 1196
    userName = testuser
    database = testdb
    schema = data_schema

    [table]
    name = sourcetable1
    table = table1
    schema = source
    host = testhost
    sqlLatestAvailableTime=select now()
    filter=1=1

    [table]
    name = desttable1
    table = table1
    schema = destination
    host = testhost

    [upload]
    name = testupload1
    sourceTable = sourcetable1
    destTable = desttable1

    [table]
    name = sourcetable2
    table = table2
    schema = source
    host = testhost
    sqlLatestAvailableTime=select now()
    filter=1=1

    [table]
    name = desttable2
    table = table2
    schema = destination
    host = testhost

    [upload]
    name = testupload2
    sourceTable = sourcetable2
    destTable = desttable2
    `
    cfg = strings.Replace(cfg, "    ", "", -1)
    // save config to file
    if err := ioutil.WriteFile(fmt.Sprintf("%v/test/config.conf", currentDir), []byte(cfg), 0700); err != nil {t.Fatal(err)}


    fmt.Println("################ TEST #1 - start dataloader")
    cmd = fmt.Sprintf("dataloader -config %v/test/config.conf -logfile %v/test/dataloader.log -verbose", currentDir,currentDir)
    fmt.Println(cmd)
    out, errout, err = executeBashCmd(cmd, nil)
    fmt.Println("out:", out, "errout:",  errout, "err:", err)
    if err != nil {
        errCount=errCount+1
        fmt.Println(errCount)
        t.Fatal(err)
        }
    fmt.Println("################ TEST #1 - done successfull")


    fmt.Println("################ TEST #2 - check table's structure in destintaion")
    sql=`SELECT
    column_name,data_type,character_maximum_length,numeric_precision,numeric_scale
    from information_schema.columns
    where table_schema='destination' and table_name='table1' order by ordinal_position`
    res, err := pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    sql=`SELECT
    column_name,data_type,character_maximum_length,numeric_precision,numeric_scale
    from information_schema.columns
    where table_schema='source' and table_name='table1' order by ordinal_position`
    resSource, err := pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    if len(res) == 0 { // if table does not exist in destination; create it
        errCount+=1
        t.Fatal(errors.New(fmt.Sprintf("Table destionation.table1 was not created after successfull run")))

    } else {
        for i := range res {
            for j := range res[i] {
                if res[i][j] != resSource[i][j] {
                    fmt.Println("Tables in source and destionation are different")
                    fmt.Println("source: ", resSource, "destination: ", res)
                    errCount += 1
                    t.Fatal(errors.New(fmt.Sprintf("Tables in source and destination are different")))
                }
            }
        }
    }

    sql=`SELECT
    column_name,data_type,character_maximum_length,numeric_precision,numeric_scale
    from information_schema.columns
    where table_schema='destination' and table_name='table2' order by ordinal_position`
    res, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    sql=`SELECT
    column_name,data_type,character_maximum_length,numeric_precision,numeric_scale
    from information_schema.columns
    where table_schema='source' and table_name='table2' order by ordinal_position`
    resSource, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    if len(res) == 0 { // if table does not exist in destination; create it
        errCount+=1
        t.Fatal(errors.New(fmt.Sprintf("Table destionation.table2 was not created after successfull run")))

    } else {
        for i := range res {
            for j := range res[i] {
                if res[i][j] != resSource[i][j] {
                    fmt.Println("Tables in source and destionation are different")
                    fmt.Println("source: ", resSource, "destination: ", res)
                    errCount += 1
                    t.Fatal(errors.New(fmt.Sprintf("Tables in source and destination are different")))
                }
            }
        }
    }

    fmt.Println("################ TEST #2 - done successfull")

    fmt.Println("################ TEST #3 - check table date ranges - all tables should be loaded up till now()-1 minute")
    sql=`select count(*) from data_schema."destTableTimeRanges"
    WHERE
    "ToDate"<now()-'2 minutes'::interval or
    "ToDate" >now()
    `
    res, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    if len(res) == 0 { // if table does not exist in destination; create it
        errCount+=1
        t.Fatal(errors.New(fmt.Sprintf("Table destTableTimeRanges has some incorrect data, check")))

    }
    fmt.Println("################ TEST #3 - done successfull")






}

//TODO list of tests:
// 1- incorrect config, upload should fail
// 2 - check date ranges table when no query,query from host,query from table
// 3 - check that no upload is done if all data is loaded
// 4 - check that tables are created correctly after basic run and table date ranges filled correctly
// 5 - check second run when we added column/changed column/deleted column/changed primary key
// 6 - check load with fields/primary keys from config
// 7 - check date ranges when we use force,days,todate flags for command line (days could be from config)
// 7.1 - force todate days
// 7.2 - todate days perMonth
// 7.3 - days from config