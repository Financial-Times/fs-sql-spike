package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	_ "net/http/pprof"

	"database/sql"
	_ "github.com/lib/pq"

	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/pborman/uuid"
)

const maxIdleConns = 65556

func main() {
	app := cli.App("org-transformer", "Serve orgs from postgresql db")

	dbName := app.String(cli.StringArg{
		Name:   "DBNAME",
		Desc:   "database schema name",
		EnvVar: "FSIMPORT_DB_NAME",
	})

	app.Action = func() { run(*dbName) }

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

}

func run(dbname string) error {
	sqlDB, err := openDB(dbname)
	if err != nil {
		log.Fatal(err)
	}

	db := &orgDB{sqlDB}

	h := handlers{db}
	m := mux.NewRouter()
	m.StrictSlash(true)
	m.HandleFunc("/transformers/organisations/__ids", h.listHandler)
	m.HandleFunc("/transformers/organisations/__count", h.countHandler)
	m.HandleFunc("/transformers/organisations/{uuid}", h.idHandler)
	http.Handle("/", m)

	port := 8081
	fmt.Printf("starting http server on port %d\n", port)
	return http.ListenAndServe(fmt.Sprintf(":%d", port), nil)

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

var emptyUUID = uuid.UUID{}

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

const schema = `

CREATE TABLE fsEntity (
	FACTSET_ENTITY_ID  varchar,
	ENTITY_NAME        varchar,
	ENTITY_PROPER_NAME varchar,
	PRIMARY_SIC_CODE   varchar,
	INDUSTRY_CODE      varchar,
	SECTOR_CODE        varchar,
	ISO_COUNTRY        varchar,
	METRO_AREA         varchar,
	STATE_PROVINCE     varchar,
	ZIP_POSTAL_CODE    varchar,
	WEB_SITE           varchar,
	ENTITY_TYPE        varchar,
	ENTITY_SUB_TYPE    varchar,
	YEAR_FOUNDED       varchar,
	ISO_COUNTRY_INCORP varchar,
	ISO_COUNTRY_COR    varchar,
	NACE_CODE          varchar
);
create unique index fsEntity_fsid on fsEntity(FACTSET_ENTITY_ID);

CREATE TABLE fsStructure (
	FACTSET_ENTITY_ID                 varchar,
	FACTSET_PARENT_ENTITY_ID          varchar,
	FACTSET_ULTIMATE_PARENT_ENTITY_ID varchar
);
create unique index fsStructure_fsid on fsStructure(FACTSET_ENTITY_ID);

CREATE TABLE fsNames (
	FACTSET_ENTITY_ID varchar,
	ENTITY_NAME_TYPE  varchar,
	ENTITY_NAME_VALUE varchar
);
create index fsNames_fsid on fsNames(FACTSET_ENTITY_ID);

CREATE TABLE fsChanges (
	FACTSET_ENTITY_ID varchar,
	CHANGE_TYPE       varchar,
	CHANGE_DATE       varchar,
	OLD_VALUE         varchar,
	NEW_VALUE         varchar,
	AUDIT_TYPE        varchar,
	COMMENTS          varchar,
	AUDIT_ID          varchar
);
create index fsChanges_fsid on fsChanges(FACTSET_ENTITY_ID);

CREATE TABLE fsIdentifiers (
	FACTSET_ENTITY_ID varchar,
	ENTITY_ID_TYPE    varchar,
	ENTITY_ID_VALUE   varchar
);
create index fsIdentifiers_fsid on fsIdentifiers(FACTSET_ENTITY_ID);


CREATE TABLE uuid_to_fsid (
	UUID              varchar,
	FACTSET_ENTITY_ID varchar
);
create index uuid_uuid on uuid_to_fsid (UUID);

`
