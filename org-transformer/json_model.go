package main

type org struct {
	UUID                   string                 `json:"uuid"`
	Type                   string                 `json:"type"`
	ProperName             string                 `json:"properName"`
	PrefLabel              string                 `json:"prefLabel"`
	LegalName              string                 `json:"legalName,omitempty"`
	ShortName              string                 `json:"shortName,omitempty"`
	HiddenLabel            string                 `json:"hiddenLabel,omitempty"`
	TradeNames             []string               `json:"tradeNames,omitempty"`
	LocalNames             []string               `json:"localNames,omitempty"`
	FormerNames            []string               `json:"formerNames,omitempty"`
	Aliases                []string               `json:"aliases,omitempty"`
	IndustryClassification string                 `json:"industryClassification,omitempty"`
	ParentOrganisation     string                 `json:"parentOrganisation,omitempty"`
	AlternativeIdentifiers alternativeIdentifiers `json:"alternativeIdentifiers,omitempty"`
	PostalCode             string                 `json:"postalCode,omitempty"`
	CountryCode            string                 `json:"countryCode,omitempty"`
	CountryOfIncorporation string                 `json:"countryOfIncorporation,omitempty"`
	YearFounded            string                 `json:"yearFounded,omitempty"`
}

type alternativeIdentifiers struct {
	TME               []string `json:"TME,omitempty"`
	UUIDs             []string `json:"uuids,omitempty"`
	FactsetIdentifier string   `json:"factsetIdentifier,omitempty"`
	LeiCode           string   `json:"leiCode,omitempty"`
}
