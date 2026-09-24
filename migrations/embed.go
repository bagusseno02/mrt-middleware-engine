package migrations

import _ "embed"

//go:embed 001_init.up.sql
var Initial string
