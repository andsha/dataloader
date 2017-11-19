package main

import (
    "fmt"
    "io/ioutil"
    "os"
    "os/exec"
    "strings"
    "testing"
    "time"

    "github.com/andsha/postgresutils"
)

func TestSingleUpload(t *testing.T) {
    // create folder for Postgres server
    currentDir,  err := os.Getwd()
    if err != nil {t.Fatal(err)}
    fmt.Println(currentDir)
    if err := os.MkdirAll(fmt.Sprintf("%v/test/pg", currentDir), 0700); err != nil {t.Fatal(err)}

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

        //remove test folder
        if err := os.RemoveAll(fmt.Sprintf("%v/test/", currentDir)); err != nil {t.Fatal(err)}

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
    sql = "CREATE SCHEMA reportspg"
    _, err = pgconn.Run(sql)
    if err != nil {t.Fatal(err)}

    // create config
    cfg := `
    [writeReportsPGDB]
    host = 127.0.0.1
    port = 1196
    userName = testuser
    database = testdb
    schema = reportspg

    [host]
    type = postgres
    name = testhost
    host = 127.0.0.1
    port = 1196
    userName = testuser
    database = testdb
    schema = reportspg

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


    fmt.Println("start dataloader")
    cmd = fmt.Sprintf("dataloader -config %v/test/config.conf", currentDir)
    fmt.Println(cmd)
    out, errout, err = executeBashCmd(cmd, nil)
    fmt.Println("out:", out, "errout:",  errout, "err:", err)
    if err != nil {t.Fatal(err)}





}
