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


    //create db
    cmd = "createdb -p 1196 testdb -U testuser"
    out, errout, err := executeBashCmd(cmd, nil)
    fmt.Println("out:", out, "errout:",  errout, "err:", err)
    //if err := exec.Command("sh", "-c", cmd).Run(); err != nil {fmt.Println(err);t.Fatalf("%v", err)}

    pgconn, err := postgresutils.NewDB("127.0.0.1", "1196", "testdb", "testuser", "", "disable", nil)
    if err != nil {t.Fatal(err)}

    res, err := pgconn.Run("SELECT 1")
    if err != nil {t.Fatal(err)}

    fmt.Println(res)

    pgconn.CloseDB()

    // stop server
    cmd = fmt.Sprintf("pg_ctl -D %v/test/pg -U testuser stop", currentDir)
    if err := exec.Command("sh", "-c", cmd).Run(); err != nil {t.Fatal(err)}
    time.Sleep(time.Second*5)

    //remove test folder
    if err := os.RemoveAll(fmt.Sprintf("%v/test/", currentDir)); err != nil {t.Fatal(err)}


}
