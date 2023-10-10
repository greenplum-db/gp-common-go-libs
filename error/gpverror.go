package error

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/constants"
)

type Error interface {
	error
	GetCode() constants.ErrorCode
	GetErr() error
}

func New(errorCode constants.ErrorCode, args ...any) Error {
	errorFormat := getErrorFormat(errorCode)
	return &error{errorCode, fmt.Errorf(errorFormat, args...)}
}

type error struct {
	constants.ErrorCode
	Err error
}

func (e *error) Error() string {
	return fmt.Sprintf("ERROR[%04d] %s", e.GetCode(), e.Err.Error())
}

func (e *error) GetCode() constants.ErrorCode {
	return e.ErrorCode
}

func (e *error) GetErr() error {
	return e.Err
}

func NewInternalError(format string, args ...any) Error {
	return &InternalError{Err: fmt.Errorf(format, args...)}
}

type InternalError struct {
	Err error
}

func (e *InternalError) Error() string {
	return e.Err.Error()
}

func (e *InternalError) GetCode() constants.ErrorCode {
	return constants.ErrorCode(9999)
}

func (e *InternalError) GetErr() error {
	return e.Err
}

// nolint:gocyclo
func getErrorFormat(errorCode constants.ErrorCode) string {
	switch errorCode {
	case NotImplemented:
		return "not implemented"
	case UnhandledError:
		return "unhandled error: %w"
	case InvalidValue:
		return `invalid value: '%v'`
	case ConfigFileNotFound:
		return fmt.Sprintf("configuration file not found; please run \"%s config init\"", constants.ExecutableName)
	case FailedToGetUserInput:
		return "failed to get user input: %w"
	case FileSystemIssue:
		return "unexpected file system issue: %w"
	case InvalidNumberOfArguments:
		return "invalid number of arguments: expected %d, but got %d"
	case GovmomiError:
		return "govmomi error occurred: %w"
	case ValueOutOfRange:
		return "value out of range: %d not in [%d, %d]"
	case FailedToReadFile:
		return "unable to read file %s: %w"
	case FailedToUnmarshalFile:
		return "unable to deserialize file %s: %w"
	case ExternalCommandFailed:
		return "executing `%s` failed: %w"
	case FailedToMarshal:
		return "unexpected failure to marshal information due to: %w"
	case FailedToCreateSshSession:
		return "failed to create TLS session on VM %s: %w"
	case FailedToRunCommandOverSsh:
		return "failed to run `%s` on VM %s: %w"
	case InvalidOption:
		return `invalid value: '%s' not in %s`
	case FailedToValidateConfig:
		return "failed to validate configuration; please see output above for details"
	case FailedToTransferFile:
		return "failed to transfer file %s to %s@%s:%s due to: %w"
	case FailedToCreateHttpPostRequest:
		return "failed to create http post request: %w"
	case FailedToSendHttpPostRequest:
		return "failed to send http post request: %w"
	case UnexpectedHttpStatusCode:
		return "unexpected http status code: %d"
	case UnableToChangePassword:
		return "unable to change password for user %s"
	case ChecksumFailureForTransferredFile:
		return "the checksum of %s on %s does not match that of the local file"
	case FailedToComputeChecksumForFile:
		return "failed to compute SHA256 for %s: %w"

	// 0100 gpv config
	case InvalidUri:
		return `invalid URI: "%s"`
	case InvalidIp:
		return "invalid IPv4 address: %s"
	case WrongIpCountForMirrored:
		return "expected 2 IPs for the mirrored deployment, but received %d"
	case WrongIpCountForMirrorless:
		return "expected 1 IP for the mirrorless deployment, but received %d"
	case InvalidDeploymentType:
		return fmt.Sprintf("the given deployment type is not one of [mirrored, mirrorless]; please re-run \"%s config init\" or \"%s config set database deployment-type\"", constants.ExecutableName, constants.ExecutableName)
	case UnableToDecodePassword:
		return "unable to decode password"
	case InvalidNetmask:
		return "invalid IPv4 netmask: %s"
	case NoPublicIpsFound:
		return fmt.Sprintf("no public IPs found in config.yml. Please run \"%s config init\"", constants.ExecutableName)
	case NoNtpServersFound:
		return fmt.Sprintf("no NTP servers found in config.yml. Please run \"%s config init\"", constants.ExecutableName)
	case DefaultPasswordChangeme:
		return "cannot use 'changeme' as password"

	// 0300 gpv vsphere validate network
	case UnableToParseVcenterUri:
		return `could not parse vCenter URI "%s"`
	case InvalidNumberOfHosts:
		return "expected to find %d hosts, but found %d"
	case InvalidNumberOfNicsPerHost:
		return "expected to find %d nics on host %s, but found %d"
	case InvalidNicSpeed:
		return "expected speed of %dMbps, but got %dMbps for vmnic %s on host %s"
	case MismatchedVmnicToSwitch:
		return "invalid vmnic configuration: vmnics must be on the same switch; Vmnic %s has two different switches: %s and %s"
	case MismatchedVmnicSpeed:
		return "invalid vmnic configuration: vmnics must have same link speed; Vmnic %s has two different speeds: %dMbps and %dMbps"
	case MismatchedVmnicConfiguration:
		return "invalid vmnic configuration: %s and %s must be on the same switch"
	case InvalidVmnicCombination:
		return "invalid vmnic configuration: vmnic0/vmnic3 and vmnic1/vmnic2 cannot be on the same switch"
	case MismatchedSwitchToDatacenter:
		return "switch %s was found, but is not in datacenter %s"
	case UnableToExtractSwitchConfigInfo:
		return "unable to extract configuration for vSphere Distributed Virtual Switch %s"
	case UnableToRetrieveSwitch:
		return "unable to retrieve vSphere Distributed Virtual Switch %s: %w"
	case NoSwitchFound:
		return "unable to find vSphere Distributed Virtual Switch %s"
	case SwitchNotUnique:
		return "vSphere Distributed Switch %s is not unique"
	case UnableToRetrieveDatacenter:
		return "unable to retrieve datacenter %s: %w"
	case UnableToRetrieveHosts:
		return "unable to retrieve hosts from cluster %s: %w"
	case NoHostsFound:
		return "no hosts found in cluster %s"
	case ClusterNotFound:
		return "cannot find cluster %s on datacenter %s"
	case PhysicalNicInfoNotFound:
		return "unable to find network information for physical nic %s"
	case WrongNetworkTypeForSwitch:
		return "%s is not a vSphere Distributed Virtual Switch"
	case UnableToRetrieveSwitchProperties:
		return "unable to fetch properties for vSphere Distributed Virtual Switch %s: %w"
	case UnableToRetrieveHostDetails:
		return "unable to fetch host details for cluster %s: %w"
	case FailedToLogin:
		return "failed to login to vCenter %s: %w"
	case FailedToPowerOnVm:
		return "failed to power-on VM %s: %w"
	case FailedToLoginToVm:
		return "failed to login to VM %s: %w"
	case FailedToShutdownVm:
		return "failed to shutdown VM %s: %w"
	case FailedToReadPassword:
		return "failed to read password: %w"

	// 1400 gpv greenplum deploy
	case FailedToRetrieveSegmentCount:
		return "unable to retrieve number of segments from Greenplum"
	case FailedToMarshalJson:
		return "unexpected failure to marshal JSON due to: %w"
	case FailedToValidateGreenplumDeployment:
		return "failed to validate greenplum deployment due to %s"
	case IncorrectSegmentCount:
		return "the number of segments and masters does not match the config: expected %s, but got %s"
	case FailedToParseCidr:
		return "failed to parse CIDR: %s"
	case BaseVmAlreadyExists:
		return "failed to clone template: base VM %s already exists"

	// 1500 gpv boot
	case OvfPropertyNotFound:
		return "ovf property with key=%s cannot be found"
	case EthernetDeviceNotFound:
		return "ethernet device not found"

	// 1600 gpv stress
	case FailedToAddEndpointCredential:
		return "failed to add stress testing endpoint credential due to: %w"
	case FailedToAddRemoteMachineCredential:
		return "failed to add stress testing remote machine credential due to: %w"
	case FailedToKillPostgresProcess:
		return "failed to kill the postgres process due to: %w"
	case FailedToGetGpdbProcessIdentifier:
		return "unable to extract Greenplum process identifier from package name: %s"
	case UnexpectedMangleResponse:
		return "unexpected mangle response with status code %d: %s"
	}

	return "unknown error"
}
