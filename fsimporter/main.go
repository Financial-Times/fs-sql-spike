package main

import (
	"archive/zip"
	"bufio"
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "net/http/pprof"

	"golang.org/x/text/encoding/charmap"

	_ "github.com/lib/pq"
	"github.com/pborman/uuid"

	"github.com/jawher/mow.cli"
)

const maxIdleConns = 65556

func main() {
	app := cli.App("fsimporter", "Import factset data into a relational db")

	edmPath := app.String(cli.StringArg{
		Name:   "EDMPATH",
		Desc:   "Full path of edm file.  E.g., /tmp/edm_premium_full_1617.zip",
		EnvVar: "FSIMPORT_EDM_PATH",
	})

	dbName := app.String(cli.StringArg{
		Name:   "DBNAME",
		Desc:   "database schema name",
		EnvVar: "FSIMPORT_DB_NAME",
	})

	app.Action = func() { run(*edmPath, *dbName) }

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(edmPath string, dbName string) {
	db, err := createAndOpenDB(dbName)
	if err != nil {
		log.Fatal(err)
	}

	err = loadAll(edmPath, db)
	if err != nil {
		log.Fatal(err)
	}

}

func createAndOpenDB(schemaName string) (*sql.DB, error) {
	if err := createDB(schemaName); err != nil {
		return nil, err
	}
	return openDB(schemaName)
}

func createDB(schemaName string) (err error) {
	var db *sql.DB
	db, err = sql.Open("postgres", "postgres://postgres:password@localhost/?sslmode=disable")
	if err != nil {
		return
	}

	defer func() {
		closeErr := db.Close()
		if err == nil {
			err = closeErr
		}
	}()

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s;", schemaName))

	return
}

func openDB(schemaName string) (*sql.DB, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("postgres://postgres:password@localhost/%s?sslmode=disable", schemaName))
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		db.Close()
	}
	db.SetMaxIdleConns(maxIdleConns)
	return db, err
}

var counter = make(chan struct{}, 65535)

func loadAll(fsFilename string, db *sql.DB) error {

	for _, stmt := range schema {
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	go func() {
		count := 0
		ticker := time.NewTicker(5 * time.Second)
		start := time.Now()
		for {
			select {
			case <-ticker.C:
				dur := time.Now().Sub(start)
				rate := float64(count) / dur.Seconds()

				log.Printf("count is %d. rate is %v\n", count, rate)
			case _, ok := <-counter:
				if !ok {
					return
				}
				count++
			}
		}
	}()

	r, err := zip.OpenReader(fsFilename)
	if err != nil {
		return err
	}
	defer r.Close()

	wg := sync.WaitGroup{}
	for _, file := range r.File {
		mapper := mappers[file.Name]
		if mapper != nil {
			wg.Add(1)
			go func(file *zip.File, f func(db *sql.DB, f *zip.File)) {
				defer wg.Done()
				f(db, file)
			}(file, mapper)
		} else {
			fmt.Fprintf(os.Stderr, "we have no use for %s\n", file.Name)
		}
	}

	wg.Wait()

	close(counter)

	log.Println("creating uuid mapping")
	fsids := make(chan string, 1024)

	// make uuid mappings
	go func() {
		defer close(fsids)
		count, err := db.Query("SELECT FACTSET_ENTITY_ID FROM fsEntity;")
		if err != nil {
			panic(err)
		}
		defer count.Close()
		for count.Next() {
			var fsid string
			err := count.Scan(&fsid)
			if err != nil {
				panic(err)
			}
			fsids <- fsid
		}
		if count.Err() != nil {
			panic(err)
		}
	}()

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	s, err := tx.Prepare("INSERT into uuid_to_fsid VALUES($1, $2)")
	if err != nil {
		panic(err)
	}

	for fsid := range fsids {
		if _, err := s.Exec(uuidFromFsid(fsid), fsid); err != nil {
			panic(err)
		}
	}

	tx.Commit()
	log.Println("done uuid mapping")

	return nil
}

var mappers = map[string]func(db *sql.DB, f *zip.File){
	"edm_entity.txt":             readEntities,
	"edm_entity_structure.txt":   readStructure,
	"edm_entity_names.txt":       readNames,
	"edm_entity_changes.txt":     readChanges,
	"edm_entity_identifiers.txt": readIdentifiers,
}

var emptyUUID = uuid.UUID{}

func uuidFromFsid(fsid string) string {
	md5data := md5.Sum([]byte(fsid))
	return uuid.NewHash(md5.New(), emptyUUID, md5data[:], 3).String()
}

func icFromFsIc(fsic string) string {
	return uuid.NewHash(md5.New(), emptyUUID, []byte(fsic), 3).String()
}

type uuidMapping struct {
	UUID              string
	FACTSET_ENTITY_ID string
}

type fsEntity struct {
	FACTSET_ENTITY_ID  string
	ENTITY_NAME        string
	ENTITY_PROPER_NAME string
	PRIMARY_SIC_CODE   string
	INDUSTRY_CODE      string
	SECTOR_CODE        string
	ISO_COUNTRY        string
	METRO_AREA         string
	STATE_PROVINCE     string
	ZIP_POSTAL_CODE    string
	WEB_SITE           string
	ENTITY_TYPE        string
	ENTITY_SUB_TYPE    string
	YEAR_FOUNDED       string
	ISO_COUNTRY_INCORP string
	ISO_COUNTRY_COR    string
	NACE_CODE          string
}

type fsStructure struct {
	FACTSET_ENTITY_ID                 string
	FACTSET_PARENT_ENTITY_ID          string
	FACTSET_ULTIMATE_PARENT_ENTITY_ID string
}

type fsNames struct {
	FACTSET_ENTITY_ID string
	ENTITY_NAME_TYPE  string
	ENTITY_NAME_VALUE string
}

type fsChanges struct {
	FACTSET_ENTITY_ID string
	CHANGE_TYPE       string
	CHANGE_DATE       string
	OLD_VALUE         string
	NEW_VALUE         string
	AUDIT_TYPE        string
	COMMENTS          string
	AUDIT_ID          string
}

type fsIdentifiers struct {
	FACTSET_ENTITY_ID string
	ENTITY_ID_TYPE    string
	ENTITY_ID_VALUE   string
}

var schema = []string{
	`
CREATE TABLE fsEntity (
	FACTSET_ENTITY_ID  varchar(255),
	ENTITY_NAME        varchar(255),
	ENTITY_PROPER_NAME varchar(255),
	PRIMARY_SIC_CODE   varchar(255),
	INDUSTRY_CODE      varchar(255),
	SECTOR_CODE        varchar(255),
	ISO_COUNTRY        varchar(255),
	METRO_AREA         varchar(255),
	STATE_PROVINCE     varchar(255),
	ZIP_POSTAL_CODE    varchar(255),
	WEB_SITE           varchar(255),
	ENTITY_TYPE        varchar(255),
	ENTITY_SUB_TYPE    varchar(255),
	YEAR_FOUNDED       varchar(255),
	ISO_COUNTRY_INCORP varchar(255),
	ISO_COUNTRY_COR    varchar(255),
	NACE_CODE          varchar(255)
);`,

	`CREATE UNIQUE INDEX fsEntity_fsid ON fsEntity (FACTSET_ENTITY_ID);`,
	`CREATE TABLE fsStructure (
	FACTSET_ENTITY_ID                 varchar(255),
	FACTSET_PARENT_ENTITY_ID          varchar(255),
	FACTSET_ULTIMATE_PARENT_ENTITY_ID varchar(255)
);`,
	`create unique index fsStructure_fsid on fsStructure(FACTSET_ENTITY_ID);`,
	`
CREATE TABLE fsNames (
	FACTSET_ENTITY_ID varchar(255),
	ENTITY_NAME_TYPE  varchar(255),
	ENTITY_NAME_VALUE varchar(255)
);`,
	"create index fsNames_fsid on fsNames(FACTSET_ENTITY_ID);",

	`CREATE TABLE fsChanges (
	FACTSET_ENTITY_ID varchar(255),
	CHANGE_TYPE       varchar(255),
	CHANGE_DATE       varchar(255),
	OLD_VALUE         varchar(255),
	NEW_VALUE         varchar(255),
	AUDIT_TYPE        varchar(255),
	COMMENTS          varchar(255),
	AUDIT_ID          varchar(255)
);`,
	"create index fsChanges_fsid on fsChanges(FACTSET_ENTITY_ID);",
	`
CREATE TABLE fsIdentifiers (
	FACTSET_ENTITY_ID varchar(255),
	ENTITY_ID_TYPE    varchar(255),
	ENTITY_ID_VALUE   varchar(255)
);
`, `
create index fsIdentifiers_fsid on fsIdentifiers(FACTSET_ENTITY_ID);`,
	`
CREATE TABLE uuid_to_fsid (
	UUID              varchar(255),
	FACTSET_ENTITY_ID varchar(255)
);`,
	`
create index uuid_uuid on uuid_to_fsid (UUID);

`,
}

func readEntities(qldb *sql.DB, f *zip.File) {
	readFactset(qldb, f, `INSERT INTO fsEntity VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);`)
}

func readStructure(qldb *sql.DB, f *zip.File) {
	readFactset(qldb, f, `INSERT INTO fsStructure VALUES($1, $2, $3);`)
}

func readNames(qldb *sql.DB, f *zip.File) {
	readFactset(qldb, f, `INSERT INTO fsNames VALUES($1, $2, $3);`)
}

func readChanges(qldb *sql.DB, f *zip.File) {
	readFactset(qldb, f, `INSERT INTO fsChanges VALUES($1, $2, $3, $4, $5, $6 ,$7, $8);`)
}

func readIdentifiers(qldb *sql.DB, f *zip.File) {
	readFactset(qldb, f, `INSERT INTO fsIdentifiers VALUES($1, $2, $3);`)
}

func readFactset(db *sql.DB, f *zip.File, insertStmt string) {
	rc, err := f.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	r := charmap.Windows1252.NewDecoder().Reader(rc)

	scanner := NewScanner(r)

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	s, err := tx.Prepare(insertStmt)
	if err != nil {
		panic(err)
	}

	count := 0
	for scanner.Scan() {
		row := scanner.Row()
		if _, err := s.Exec(row...); err != nil {
			panic(err)
		}
		counter <- struct{}{}
		if count%1024 == 0 {
			if err := tx.Commit(); err != nil {
				panic(err)
			}
			tx, err = db.Begin()
			if err != nil {
				panic(err)
			}
			s, err = tx.Prepare(insertStmt)
			if err != nil {
				panic(err)
			}

		}
		count++
	}

	if err := s.Close(); err != nil {
		panic(err)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error reading input:", err)
	}

	if err := tx.Commit(); err != nil {
		panic(err)
	}

}

func NewScanner(r io.Reader) *scanner {
	s := bufio.NewScanner(bufio.NewReader(r))

	// first line is field names
	s.Scan()
	colNames := strings.Split(s.Text(), "|")
	if len(colNames) < 1 || colNames[0] != "\"FACTSET_ENTITY_ID\"" {
		panic("unexpected factset file format")
	}

	return &scanner{s}
}

type scanner struct {
	s *bufio.Scanner
}

func (s *scanner) Scan() bool {
	return s.s.Scan()
}

func (s *scanner) Row() []interface{} {
	text := s.s.Text()
	cols := strings.Split(text, "|")
	row := make([]interface{}, len(cols))
	for i, val := range cols {
		if strings.HasSuffix(val, `"`) && strings.HasPrefix(val, `"`) {
			val = val[1 : len(val)-1]
		}
		row[i] = val
	}
	return row
}

func (s *scanner) Err() error {
	return s.s.Err()
}
