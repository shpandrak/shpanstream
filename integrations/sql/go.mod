module github.com/shpandrak/shpanstream/integrations/sql

go 1.23

require (
	// test dependency only
	github.com/mattn/go-sqlite3 v1.14.32
	github.com/shpandrak/shpanstream v0.3.12
	github.com/stretchr/testify v1.11.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/shpandrak/shpanstream => ../../
