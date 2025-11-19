package pgwf

import _ "embed"

// SQL contains the pgwf schema definition for consumers who need to apply it programmatically.
//
//go:embed pgwf.sql
var SQL string
