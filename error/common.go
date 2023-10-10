package error

import (
	"github.com/greenplum-db/gp-common-go-libs/constants"
	"github.com/sirupsen/logrus"
)

// All of the status codes
// We interleave the ErrorCode and WarningCode values in order to preserve
// the uniqueness of the integer values of the codes.
const (
	NotImplemented                    = constants.ErrorCode(1)
	UnhandledError                    = constants.ErrorCode(2)
	InvalidValue                      = constants.ErrorCode(3)
	ConfigFileNotFound                = constants.ErrorCode(5)
	FailedToGetUserInput              = constants.ErrorCode(6)
	FileSystemIssue                   = constants.ErrorCode(7)
	InvalidNumberOfArguments          = constants.ErrorCode(8)
	GovmomiError                      = constants.ErrorCode(9)
	ValueOutOfRange                   = constants.ErrorCode(10)
	FailedToReadFile                  = constants.ErrorCode(11)
	FailedToUnmarshalFile             = constants.ErrorCode(12)
	ExternalCommandFailed             = constants.ErrorCode(13)
	FailedToMarshal                   = constants.ErrorCode(14)
	FailedToCreateSshSession          = constants.ErrorCode(15)
	FailedToRunCommandOverSsh         = constants.ErrorCode(16)
	InvalidOption                     = constants.ErrorCode(17)
	FailedToValidateConfig            = constants.ErrorCode(18)
	FailedToTransferFile              = constants.ErrorCode(19)
	FailedToCreateHttpPostRequest     = constants.ErrorCode(20)
	FailedToSendHttpPostRequest       = constants.ErrorCode(21)
	UnexpectedHttpStatusCode          = constants.ErrorCode(22)
	UnableToChangePassword            = constants.ErrorCode(23)
	ChecksumFailureForTransferredFile = constants.ErrorCode(25)
	FailedToComputeChecksumForFile    = constants.ErrorCode(26)

	// 0100 gpv config
	InvalidUri                     = constants.ErrorCode(101)
	InvalidIp                      = constants.ErrorCode(102)
	WrongIpCountForMirrored        = constants.ErrorCode(103)
	WrongIpCountForMirrorless      = constants.ErrorCode(104)
	InvalidDeploymentType          = constants.ErrorCode(105)
	UnableToDecodePassword         = constants.ErrorCode(106)
	TwoExternalIpAddressesRequired = constants.WarningCode(107)
	OneExternalIpAddressRequired   = constants.WarningCode(108)
	ExtraIpAddressesRemoved        = constants.WarningCode(109)
	InvalidNetmask                 = constants.ErrorCode(110)
	BaseVmIpReset                  = constants.WarningCode(111)
	BaseVmNetmaskReset             = constants.WarningCode(112)
	BaseVmGatewayIpReset           = constants.WarningCode(113)
	BaseVmSetIp                    = constants.WarningCode(114)
	BaseVmSetNetmask               = constants.WarningCode(115)
	BaseVmSetGatewayIp             = constants.WarningCode(116)
	NoPublicIpsFound               = constants.ErrorCode(117)
	NoNtpServersFound              = constants.ErrorCode(118)
	DefaultPasswordChangeme        = constants.ErrorCode(119)
	BaseVmIpWarningForDhcp         = constants.WarningCode(120)
	BaseVmNetmaskWarningForDhcp    = constants.WarningCode(121)
	BaseVmGatewayWarningForDhcp    = constants.WarningCode(122)

	// 0200 gpv vsphere validate access
	// 0300 gpv vsphere validate network
	UnableToParseVcenterUri          = constants.ErrorCode(301)
	InvalidNumberOfHosts             = constants.ErrorCode(302)
	InvalidNumberOfNicsPerHost       = constants.ErrorCode(303)
	InvalidNicSpeed                  = constants.ErrorCode(304)
	NicSpeedTooHigh                  = constants.WarningCode(305)
	MismatchedVmnicToSwitch          = constants.ErrorCode(306)
	MismatchedVmnicSpeed             = constants.ErrorCode(307)
	MismatchedVmnicConfiguration     = constants.ErrorCode(308)
	InvalidVmnicCombination          = constants.ErrorCode(309)
	LldpNotFound                     = constants.WarningCode(310)
	MismatchedSwitchToDatacenter     = constants.ErrorCode(311)
	UnableToExtractSwitchConfigInfo  = constants.ErrorCode(312)
	UnableToRetrieveSwitch           = constants.ErrorCode(313)
	NoSwitchFound                    = constants.ErrorCode(314)
	SwitchNotUnique                  = constants.ErrorCode(315)
	UnableToRetrieveDatacenter       = constants.ErrorCode(316)
	UnableToRetrieveHosts            = constants.ErrorCode(317)
	NoHostsFound                     = constants.ErrorCode(318)
	ClusterNotFound                  = constants.ErrorCode(319)
	PhysicalNicInfoNotFound          = constants.ErrorCode(320)
	WrongNetworkTypeForSwitch        = constants.ErrorCode(321)
	UnableToRetrieveSwitchProperties = constants.ErrorCode(322)
	UnableToRetrieveHostDetails      = constants.ErrorCode(323)
	FailedToLogin                    = constants.ErrorCode(324)
	FailedToPowerOnVm                = constants.ErrorCode(325)
	FailedToLoginToVm                = constants.ErrorCode(326)
	FailedToShutdownVm               = constants.ErrorCode(327)
	FailedToReadPassword             = constants.ErrorCode(328)

	// 0400 gpv vsphere validate storage
	// 0500 gpv vsphere validate ha-drs
	// 0600 gpv vsphere validate capacity
	// 0700 gpv vsphere validate performance
	// 0800 gpv greenplum validate smoke
	// 0900 gpv key-provider deploy
	// 1000 gpv network deploy
	// 1100 gpv storage deploy
	// 1200 gpv ha-drs deploy
	// 1300 gpv vms deploy

	// 1400 gpv greenplum deploy
	FailedToRetrieveSegmentCount        = constants.ErrorCode(1401)
	FailedToMarshalJson                 = constants.ErrorCode(1402)
	FailedToValidateGreenplumDeployment = constants.ErrorCode(1403)
	IncorrectSegmentCount               = constants.ErrorCode(1404)
	PackageAlreadyInstalled             = constants.WarningCode(1405)
	FailedToParseCidr                   = constants.ErrorCode(1406)
	BaseVmAlreadyExists                 = constants.ErrorCode(1407)

	// 1500 gpv boot
	OvfPropertyNotFound    = constants.ErrorCode(1501)
	EthernetDeviceNotFound = constants.ErrorCode(1502)

	// 1600 gpv stress
	FailedToAddEndpointCredential      = constants.ErrorCode(1601)
	FailedToAddRemoteMachineCredential = constants.ErrorCode(1602)
	FailedToKillPostgresProcess        = constants.ErrorCode(1603)
	MirroredStressTestNotSupported     = constants.WarningCode(1604)
	FailedToGetGpdbProcessIdentifier   = constants.ErrorCode(1605)
	UnexpectedMangleResponse           = constants.ErrorCode(1606)
)

type Logger interface {
	GetCode() int
	GetMsg() string
	String() string
	Format(e *logrus.Entry) ([]byte, error)
}
