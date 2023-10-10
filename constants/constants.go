package constants

const DefaultGpvPassword = "changeme"
const ExecutableName = "gpv"
const GpvPath = "/etc/gpv"

type DeploymentType string

const (
	Mirrored   DeploymentType = "mirrored"
	Mirrorless DeploymentType = "mirrorless"
)

var AllDeploymentTypes = []DeploymentType{
	Mirrored,
	Mirrorless,
}

type ErrorCode uint32
type WarningCode uint32

type NetworkType string

const (
	Dhcp   NetworkType = "dhcp"
	Static NetworkType = "static"
)

var AllNetworkTypes = []NetworkType{
	Dhcp,
	Static,
}

type StorageType string

const (
	Powerflex StorageType = "powerflex"
	Vsan      StorageType = "vsan"
)

var AllStorageTypes = []StorageType{
	Powerflex,
	Vsan,
}

type YesNo string

const (
	No  YesNo = "no"
	Yes YesNo = "yes"
)

var AllYesNos = []YesNo{
	No,
	Yes,
}

type OptionType interface {
	DeploymentType | NetworkType | StorageType | YesNo
}

type SettingType interface {
	int | string | []string | OptionType
}
