// extension for vcongig library special for dataloader

package main

import (
	"errors"
	"fmt"
    "strings"

	"github.com/andsha/postgresutils"
	"github.com/andsha/vconfig"
)

// ******************  extenions for section ************************

type lSection struct {
	vconfig.Section
}

func (sec *lSection) getDBConnParameters() (map[string]string, error) {
	dbParams := make(map[string]string)
	host, err := sec.GetSingleValue("host", "")
	if err != nil {return nil, err}
	dbParams["host"] = host
	port, err := sec.GetSingleValue("port", "5432")
	if err != nil {return nil, err}
	dbParams["port"] = port
	database, err := sec.GetSingleValue("database", "")
	if err != nil {return nil, err}
	dbParams["database"] = database
	user, err := sec.GetSingleValue("userName", "")
	if err != nil {return nil, err}
	dbParams["user"] = user
	password, err := sec.GetSingleValue("password", "5432")
	if err != nil {return nil, err}
	dbParams["password"] = password

	return dbParams, nil
}

// ***********************  extension to vcongig *********************

type lvconfig struct {
	vconfig.VConfig
}

func (lvc *lvconfig) updateDBUploads() error {
	dbuSections, err := lvc.GetSections("dbUpload")
	if err != nil {return err}

	for _, dbuSection := range dbuSections {
        sourceHostName, err := dbuSection.GetSingleValue("sourceHost", "")
		if err != nil {return err}
        //Create DB for this source and destination
        pgprocess, err := lvc.getPGConn(sourceHostName)
        if err != nil {return err}
		tables, err := lvc.getTables(dbuSection, pgprocess)
        if err != nil{return err}

        for _, table := range tables{
            // Add source table
    		sec, err := lvc.createSourceTableSection(dbuSection, table, pgprocess)
    		if err != nil {return err}
    		lvc.AddSection(sec)
    		// add destination table
    		sec, err = lvc.createDestTableSection(dbuSection, table, pgprocess)
    		if err != nil {return err}
    		lvc.AddSection(sec)
            // add upload
            sec, err = lvc.createUploadSection(dbuSection, table)
    		if err != nil {return err}
    		lvc.AddSection(sec)
        }
        pgprocess.CloseDB()
	}
	return nil
}

func (lvc *lvconfig) getPGConn(hostName string) (*postgresutils.PostgresProcess, error){
    if hostName != "reportspgdb-ha" {return nil, errors.New(fmt.Sprintf("Cannot get tables from %v", hostName))}
		// find source host info
		sourceHosts, err := lvc.GetSections("host")
		if err != nil {return nil, err}
		sourceHost := new(vconfig.Section)
		for _, sh := range sourceHosts {
			shn, err := sh.GetSingleValue("name", "")
			if err != nil {return nil, err}
			if shn == hostName {
				sourceHost = sh
				break
			}
		}
		if sourceHost == nil {return nil, errors.New(fmt.Sprintf("Cannot find host %v", hostName))}
		// create connection to source
		var lsourceHost lSection
		lsourceHost.Section = *sourceHost
		dbPrams, err := lsourceHost.getDBConnParameters()
		if err != nil {return nil, err}
		pwdSection, err := lvc.GetSections("SECURE PASSWORD")
		if err != nil {return nil, err}
		pgprocess, err := postgresutils.NewDB(dbPrams["host"],
			dbPrams["port"],
			dbPrams["database"],
			dbPrams["user"],
			dbPrams["password"],
			"disable",
			pwdSection[0])
		if err != nil {return nil, err}

        return pgprocess, nil
}

func (lvc *lvconfig) getTables (dbuSection *vconfig.Section, pgprocess *postgresutils.PostgresProcess ) ([]string, error){
    tables := make([]string, 0)
        // get list of tables
		if ts, err := dbuSection.GetSingleValue("tables", ""); err != nil {
			//when do not have tables in dbUploads, get list of tables in Postgres
			schema, err := dbuSection.GetSingleValue("db", "")
			if err != nil {return nil, err}
			sql := fmt.Sprintf("SELECT table_name from information_schema.tables where table_schema='%v'", schema)
			res, err := pgprocess.Run(sql)
			if err != nil {return nil, err}
            for _,r:=range res[0]{
                if s, ok := r.(string); ok {tables = append(tables, s)} else{return nil, errors.New(fmt.Sprintf("Error while adding '%v' table ", r))}
            }
		} else {
			tables = strings.Split(ts, ",")
		}
        return tables, nil
}

func (lvc *lvconfig) createSourceTableSection(usection *vconfig.Section, table string, pgprocess *postgresutils.PostgresProcess) (*vconfig.Section, error) {
	sec := vconfig.NewSection("table")
    sec.SetValues("table", []string{table})
    schema, err := usection.GetSingleValue("db", "")
    if err != nil {return nil, err}
    sec.SetValues("schema", []string{schema})
    name := []string{fmt.Sprintf("%v.%v", schema, table)}
    sec.SetValues("name", name)
    host, _ := usection.GetSingleValue("sourceHost", "")
    sec.SetValues("host", []string{host})
    filter, _ := usection.GetSingleValue("filter", "")
    fullReload, _ := usection.GetSingleValue("fullReload", "")
    if fullReload == "True" || fullReload == "true" {filter = "1=1"}
    if filter == "" {
        timestampField, filter := getTimestampFieldAndFilter(pgprocess, table, schema)
        if timestampField != "" {
            sec.SetValues("timestampField", []string{timestampField})
        }
        if filter != "" {
            sec.SetValues("filter", []string{filter})
        }
    } else {
        sec.SetValues("filter", []string{filter})
    }
    if timestampFieldForCheck := getTimestampFieldForCheck(pgprocess, table, schema); timestampFieldForCheck != ""{
        sec.SetValues("timestampFieldForCheck", []string{timestampFieldForCheck})
    }
    if excludedFields, _ := usection.GetSingleValue("excludedFields", ""); excludedFields != ""{
        sec.SetValues("excludedFields", []string{excludedFields})
    }
    if addMD5ToFields, _ := usection.GetSingleValue("addMD5ToFields", ""); addMD5ToFields != ""{
        sec.SetValues("addMD5ToFields", []string{addMD5ToFields})
    }
    if primary_key, _ := usection.GetSingleValue("primary_key", ""); primary_key != ""{
        sec.SetValues("primary_key", []string{primary_key})
    }
    if sqlLatestAvailableTime, _ := usection.GetSingleValue("sqlLatestAvailableTime", ""); sqlLatestAvailableTime != ""{
        sec.SetValues("sqlLatestAvailableTime", []string{sqlLatestAvailableTime})
    }

	return sec, nil
}

func (lvc *lvconfig) createDestTableSection(usection *vconfig.Section, table string, pgprocess *postgresutils.PostgresProcess) (*vconfig.Section, error) {
    sec := vconfig.NewSection("table")
    schema, err := usection.GetSingleValue("db", "")
    if err != nil {return nil, err}
    sec.SetValues("schema", []string{schema})
    name := []string{fmt.Sprintf("bfd_%v_%v", schema, table)}
    sec.SetValues("name", name)
    host, _ := usection.GetSingleValue("destHost", "")
    sec.SetValues("host", []string{host})
    destTable := fmt.Sprintf("%v_%v", schema, toLowerAndNoPrefix(table))
    if table == "studioShareDetails" && schema == "reportspg"{destTable = "reportspg_purchases"}
    sec.SetValues("table", []string{destTable})
    if timestampFieldForCheck := getTimestampFieldForCheck(pgprocess, table, schema); timestampFieldForCheck != ""{
        sec.SetValues("timestampFieldForCheck", []string{toLowerAndNoPrefix(timestampFieldForCheck)})
    }
    if partitionBy, _ := usection.GetSingleValue("partitionBy", ""); partitionBy != "" {
        sec.SetValues("partitionBy", []string{partitionBy})
    }
    return sec, nil
}

func (lvc *lvconfig) createUploadSection(usection *vconfig.Section, table string) (*vconfig.Section, error){
    sec := vconfig.NewSection("upload")
    schema, err := usection.GetSingleValue("db", "")
    if err != nil {return nil, err}
    sec.SetValues("name", []string{fmt.Sprintf("from_%v_%v_to_bfd", schema, table)})
    sec.SetValues("sourceTable", []string{fmt.Sprintf("%v_%v", schema, table)})
    sec.SetValues("destTable", []string{fmt.Sprintf("bfd_%v_%v", schema, table)})
    sec.SetValues("fieldMap", []string{"sourceFields"})
    deleteFile, _ := usection.GetSingleValue("deleteFile", "False")
    sec.SetValues("deleteFile", []string{deleteFile})
    perMonth, _ := usection.GetSingleValue("perMonth", "False")
    sec.SetValues("perMonth", []string{perMonth})
    fileFormat, _ := usection.GetSingleValue("fileFormat", "csv")
    sec.SetValues("fileFormat", []string{fileFormat})
    for _, field := range []string{"startDate", "stopDate", "filter", "fullReload",
                                    "doCheck", "doCheckChecksum", "checkDays",
                                    "possibleCheckDiscrepancy",  "checkSourceQuery",
                                    "checkDestQuery", "daysToLoad"}{
        if v, _ := sec.GetSingleValue(field, ""); v != "" {sec.SetValues(field, []string{v})}
    }
    return sec, nil
}


func getTimestampFieldAndFilter(pgprocess *postgresutils.PostgresProcess, table string, schema string) (string, string){
    if table == "accountDetails" && schema == "reportspg"{
        return "", `greatest("createdTime","closedTime","activationTransactionTimestampPST","FirstAfterActivationTransactionTimestampPST","FirstPaidPurchaseTimestampPST")>='##{v_startDate}'`
    }
    if table == "instaWatchTransactions" && schema == "reportspg"{
        return "", `greatest("vuduPurchaseTimePstHelperId","uvExportTimePstHelperId","refundTimePstHelperId")>=(select "timeHelperId" from reportspg."timeHelpersPst" where "helperDate"='##{v_startDate}')`
    }
    if table == "userBitrates" && schema == "aggstats"{
            return "", `("userId","sessionId") in (select "userId","sessionId" from aggstats."userSessions" where "modificationTime">='##{v_startDate}')`
    }

    sql := fmt.Sprintf("SELECT column_name from information_schema.columns where table_name='%s' and table_schema='%s'", table, schema)
    res, err := pgprocess.Run(sql)
    if len(res) > 0 {
        if err != nil {return "", ""}
        for _, s := range []string{"purchaseTimePstId","refundTimePstId","streamingDatePstId"}{
            for _, r := range res[0]{
                if r == s{
                    return "", fmt.Sprintf(`"%v" in (select "timeHelperId" from reportspg."timeHelpersPst" where "helperDate" >= '##{v_startDate}' and "helperDate"<'##{v_stopDate}')`, r)
                }
            }
        }
        for _, s := range []string{"purchaseDatePst","modificationTime","purchaseTime", "purchasedTime","recordDate","monthPST","streamDatePst","metricTimestampPST","creationTime","startTime"}{
            for _, r := range res[0]{
                if r == s{
                    return s, ""
                }
            }
        }
        f := 0
        for _, s := range []string{"accountId", "purchaseId"}{
            for _, r := range res[0]{
                if r == s{
                    f++
                }
            }
        }
        if f == 2{
            return "", `("accountId","purchaseId") in (select "accountId","purchaseId" from reportspg."purchasesInfo" where "purchaseTimePstId" in (select "timeHelperId" from reportspg."timeHelpersPst" where "helperDate" >= '##{v_startDate}' and "helperDate"<'##{v_stopDate}'))`
        }
    }

    return "", "1=1"
}

func getTimestampFieldForCheck(pgprocess *postgresutils.PostgresProcess, table string, schema string) string {
    sql := fmt.Sprintf("SELECT column_name from information_schema.columns where table_name='%v' and table_schema='%v'", table, schema)
    res, err := pgprocess.Run(sql)
    if len(res) > 0 {
        if err != nil {return ""}
        for _, s := range []string{"purchaseDatePst","purchaseTime", "purchasedTime","recordDate","monthPST","streamDatePst","metricTimestampPST","creationTime","startTime","createdTime","vuduPurchaseTime","linkCreationTime","purchaseTimePstId", "modificationTime","time"}{
            for _, r := range res[0]{
                if r == s{
                    return s
                }
            }
        }

        f := 0
        for _, s := range []string{"accountId", "purchaseId"}{
            for _, r := range res[0]{
                if r == s{
                    f++
                }
            }
        }
        if f == 2{
            return `("accountId","purchaseId") `
        }
    }

    return ""
}

func toLowerAndNoPrefix(str string) string{
    s := strings.ToLower(str)
    s = strings.TrimLeft(s, "_")
    s = strings.Replace(s, "p_s_t", "pst", -1)
    return s
}
