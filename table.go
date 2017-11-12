package main

import (
	"errors"
    "fmt"
    "strings"
    "time"

	"github.com/andsha/vconfig"
)

type table interface {
    getGenericTableDescription() ([]tableDescription, error)
    getTableDescription() ([]tableDescription, error)
    getAvailabeDataTimeRanges() ([][]time.Time, error)
    checkTable(descrition []tableDescription) error
    connect() error
    cleanup() error
    disconnect() error
    getData([]time.Time) ([]byte, error)
    updateTimeRanges(time.Time, time.Time, bool) error
    uploadData([]byte) error
}

type generictable struct {
    tablesection    *vconfig.Section
    upload      *upload
    tempFiles []string
}

func NewTable(tsec *vconfig.Section, ul *upload) (table, error) {
	tHostName, err := tsec.GetSingleValue("host", "")
	if err != nil {return nil, err}
    tHostSections, err := ul.m_vconfig.GetSectionsByVar("host", "name", tHostName)
    if err != nil {return nil, err}
    tType, err := tHostSections[0].GetSingleValue("type", "")
    if err != nil {return nil, err}
    ct := new(generictable)
    ct.tablesection = tsec
    ct.upload = ul

    switch tType {
	case "postgres":
		t := new(pgtable)
        t.generictable = *ct
		return t, nil
	default:
		return nil, errors.New(fmt.Sprintf("No such type of table %v", tType))
	}
}

func (gt *generictable) getGenericTableDescription () ([]tableDescription, error) {
    var description []tableDescription
    gt.upload.m_logger.Debug(gt.upload.m_sec)
    if fieldMap, _ := gt.upload.m_sec.GetSingleValue("fieldMap", ""); fieldMap != "" {
        gt.upload.m_logger.Debug(fieldMap)
        // get name of destination table
        destTableName, _ := gt.upload.m_sec.GetSingleValue("destTable", "")

        //get destination table section
        destinations, _ := gt.upload.m_vconfig.GetSectionsByVar("table", "name", destTableName)

        //Check Fields in destination table
        if fields, _ :=  destinations[0].GetSingleValue("fields", ""); fields != "" {
            maps := strings.Split(fieldMap, ",")

            // extract source and dest field names
            for _, fmap := range maps {
                var d tableDescription
                s := strings.Split(fmap, ":")
                d.sourceName = strings.Trim(s[0], " ")
                d.destName = strings.Trim(s[1], " ")


                // extract destination field types
                for _, field := range strings.Split(fields, ",") {
                    s := strings.Split(field, ":")
                    destName := strings.Trim(s[0], " ")
                    if destName == d.destName {
                        d.destType = strings.Trim(s[1], " ")
                    }
                }

                // extract destination field primary keys
                if pkeys, _ := destinations[0].GetSingleValue("PrimaryKey", ""); pkeys != "" {
                    for _, pk := range strings.Split(pkeys, ",") {
                        pkey := strings.Trim(pk, " ")
                        if pkey == d.destName {
                            d.isPKey = true
                        }
                    }
                }

                description = append(description, d)
            }
        } else {
            return nil, nil
        }

    }

    return description, nil
}
