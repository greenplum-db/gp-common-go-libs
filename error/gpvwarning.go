package error

import (
	"github.com/greenplum-db/gp-common-go-libs/constants"
)

// nolint:gocyclo
func GetWarningFormat(statusCode constants.WarningCode) string {
	switch statusCode {
	// 0100 gpv config
	case TwoExternalIpAddressesRequired:
		return `please ensure that you have two external IP addresses for mirrored deployments`
	case OneExternalIpAddressRequired:
		return `please ensure that you have one external IP address for mirrorless deployments`
	case ExtraIpAddressesRemoved:
		return `removed %s from the list of gp-virtual-external IPs`
	case BaseVmIpReset:
		return `base VM IP set to 0.0.0.0`
	case BaseVmNetmaskReset:
		return `base VM IP netmask set to 0.0.0.0`
	case BaseVmGatewayIpReset:
		return `base VM gateway IP set to 0.0.0.0`
	case BaseVmSetIp:
		return `please set base VM IP`
	case BaseVmSetNetmask:
		return `please set base VM IP netmask`
	case BaseVmSetGatewayIp:
		return `please set base VM gateway IP`
	case BaseVmIpWarningForDhcp:
		return `IP for given network type cannot be changed`
	case BaseVmNetmaskWarningForDhcp:
		return `Netmask for given network type cannot be changed`
	case BaseVmGatewayWarningForDhcp:
		return `Gateway for given network type cannot be changed`

	// 0300 gpv vsphere validate network
	case NicSpeedTooHigh:
		return `vmnic "%s" of host "%s" speed is too high. Expected speed: %dMbps; actual speed: %dMbps.`
	case LldpNotFound:
		return `LLDP is not enabled on the vmnic "%s" on host "%s"`

	// 1400 gpv greenplum deploy
	case PackageAlreadyInstalled:
		return `%s already installed`

	// 1600 gpv stress test
	case MirroredStressTestNotSupported:
		return `Stress test not supported for Mirrored Deployment`
	}

	return "unknown warning"
}
