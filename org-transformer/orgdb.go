package main

import (
	"crypto/md5"
	"database/sql"
	"log"

	"github.com/pborman/uuid"
)

type orgDB struct {
	db *sql.DB
}

func (orgs *orgDB) getOrg(uuid string) (o org, found bool, err error) {

	// uuid to fsid mapping
	mapRows, err := orgs.db.Query(`SELECT * from uuid_to_fsid WHERE UUID = $1;`, uuid)
	if err != nil {
		return
	}
	defer mapRows.Close()

	if !mapRows.Next() {
		return
	}
	if mapRows.Err() != nil {
		panic(mapRows.Err)
	}

	var u uuidMapping
	err = mapRows.Scan(
		&u.UUID,
		&u.FACTSET_ENTITY_ID,
	)
	if err != nil {
		panic(err)
	}

	// entity
	entRows, err := orgs.db.Query(`SELECT * from fsEntity WHERE FACTSET_ENTITY_ID = $1;`, u.FACTSET_ENTITY_ID)
	if err != nil {
		return
	}
	defer entRows.Close()

	if !entRows.Next() {
		// this is a bug. log something or something?
		return
	}
	if entRows.Err() != nil {
		panic(entRows.Err)
	}

	found = true

	var e fsEntity
	err = entRows.Scan(
		&e.FACTSET_ENTITY_ID,
		&e.ENTITY_NAME,
		&e.ENTITY_PROPER_NAME,
		&e.PRIMARY_SIC_CODE,
		&e.INDUSTRY_CODE,
		&e.SECTOR_CODE,
		&e.ISO_COUNTRY,
		&e.METRO_AREA,
		&e.STATE_PROVINCE,
		&e.ZIP_POSTAL_CODE,
		&e.WEB_SITE,
		&e.ENTITY_TYPE,
		&e.ENTITY_SUB_TYPE,
		&e.YEAR_FOUNDED,
		&e.ISO_COUNTRY_INCORP,
		&e.ISO_COUNTRY_COR,
		&e.NACE_CODE,
	)
	if err != nil {
		panic(err)
	}

	o.UUID = u.UUID
	switch e.ENTITY_TYPE {
	case "PUB":
		o.Type = "PublicCompany"
	case "EXT":
		//o.Extinct = true
		o.Type = "Organisation"
	default:
		o.Type = "Organisation"
	}

	o.PrefLabel = e.ENTITY_PROPER_NAME
	o.ProperName = e.ENTITY_PROPER_NAME
	o.HiddenLabel = e.ENTITY_NAME

	o.YearFounded = e.YEAR_FOUNDED

	o.AlternativeIdentifiers = alternativeIdentifiers{
		FactsetIdentifier: e.FACTSET_ENTITY_ID,
		UUIDs:             []string{u.UUID},
	}

	if e.INDUSTRY_CODE != "" {
		o.IndustryClassification = icFromFsIc(e.INDUSTRY_CODE)
	}

	o.PostalCode = e.ZIP_POSTAL_CODE
	o.CountryCode = e.ISO_COUNTRY
	o.CountryOfIncorporation = e.ISO_COUNTRY_INCORP

	// structure
	structRows, err := orgs.db.Query(`SELECT * from fsStructure WHERE FACTSET_ENTITY_ID = $1;`, e.FACTSET_ENTITY_ID)
	if err != nil {
		panic(err)
	}
	defer structRows.Close()

	if structRows.Next() {
		var structure fsStructure
		if err := structRows.Scan(
			&structure.FACTSET_ENTITY_ID,
			&structure.FACTSET_PARENT_ENTITY_ID,
			&structure.FACTSET_ULTIMATE_PARENT_ENTITY_ID,
		); err != nil {
			panic(err)
		}

		if structure.FACTSET_PARENT_ENTITY_ID != "" {
			o.ParentOrganisation = uuidFromFsid(structure.FACTSET_PARENT_ENTITY_ID)
		}
	}

	// names
	nameRows, err := orgs.db.Query(`SELECT * from fsNames WHERE FACTSET_ENTITY_ID = $1;`, e.FACTSET_ENTITY_ID)
	if err != nil {
		panic(err)
	}
	defer nameRows.Close()

	for nameRows.Next() {
		var nr fsNames
		if err := nameRows.Scan(
			&nr.FACTSET_ENTITY_ID,
			&nr.ENTITY_NAME_TYPE,
			&nr.ENTITY_NAME_VALUE,
		); err != nil {
			panic(err)
		}
		switch nr.ENTITY_NAME_TYPE {
		case "FORMER_NAME":
			o.FormerNames = append(o.FormerNames, nr.ENTITY_NAME_VALUE)
		case "SHORT_NAME":
			o.ShortName = nr.ENTITY_NAME_VALUE
		case "LEGAL_NAME":
			o.LegalName = nr.ENTITY_NAME_VALUE
		case "TRADE_DBA_NAME":
			o.TradeNames = append(o.TradeNames, nr.ENTITY_NAME_VALUE)
		case "LOCAL_NAME":
			o.LocalNames = append(o.LocalNames, nr.ENTITY_NAME_VALUE)
		default:
			log.Printf("unknown name type %v - skipping\n", nr.ENTITY_NAME_TYPE)
		}
	}
	if nameRows.Err() != nil {
		panic(nameRows.Err())
	}

	// changes
	changeRows, err := orgs.db.Query(`SELECT * from fsChanges WHERE FACTSET_ENTITY_ID = $1;`, e.FACTSET_ENTITY_ID)
	if err != nil {
		panic(err)
	}
	defer changeRows.Close()
	for changeRows.Next() {
		//TODO
	}

	// identifiers
	idRows, err := orgs.db.Query(`SELECT * from fsIdentifiers WHERE FACTSET_ENTITY_ID = $1;`, e.FACTSET_ENTITY_ID)
	if err != nil {
		panic(err)
	}
	defer idRows.Close()
	for idRows.Next() {
		var ident fsIdentifiers
		if err := idRows.Scan(
			&ident.FACTSET_ENTITY_ID,
			&ident.ENTITY_ID_TYPE,
			&ident.ENTITY_ID_VALUE,
		); err != nil {
			panic(err)
		}
		switch ident.ENTITY_ID_TYPE {
		case "LEI":
			o.AlternativeIdentifiers.LeiCode = ident.ENTITY_ID_VALUE
		default:
			log.Printf("unknown identifier type %s. skipping.\n", ident.ENTITY_ID_TYPE)
		}

	}

	return
}

func icFromFsIc(fsic string) string {
	return uuid.NewHash(md5.New(), emptyUUID, []byte(fsic), 3).String()
}

func uuidFromFsid(fsid string) string {
	md5data := md5.Sum([]byte(fsid))
	return uuid.NewHash(md5.New(), emptyUUID, md5data[:], 3).String()
}

func (orgs *orgDB) size() (int, error) {
	count, err := orgs.db.Query("SELECT count(*) FROM fsEntity;")
	if err != nil {
		panic(err)
	}
	defer count.Close()
	if count.Next() {
		var i int
		err := count.Scan(&i)
		if err != nil {
			panic(err)
		}
		return i, nil
	}
	panic(count.Err())
}

func (orgs *orgDB) forEachId(f func(id string) error) error {
	q, err := orgs.db.Query("SELECT UUID FROM uuid_to_fsid;")
	if err != nil {
		panic(err)
	}
	defer q.Close()
	for q.Next() {
		var s string
		err := q.Scan(&s)
		if err != nil {
			return err
		}
		err = f(s)
		if err != nil {
			return err
		}
	}
	return nil
}
