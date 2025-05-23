package main

import (
	_ "embed"

	"github.com/mbreese/batchq/cmd"
)

//go:embed LICENSE
var licenseText string

func main() {
	cmd.SetLicenseText(licenseText)
	cmd.Execute()
}
