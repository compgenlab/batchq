package main

import (
	_ "embed"

	"github.com/compgenlab/batchq/cmd"
)

//go:embed LICENSE
var licenseText string

func main() {
	cmd.SetLicenseText(licenseText)
	cmd.Execute()
}
