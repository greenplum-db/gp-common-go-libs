package error_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	"github.com/greenplum-db/gp-common-go-libs/constants"
	"github.com/greenplum-db/gp-common-go-libs/error"
)

var _ = Describe("warning", func() {
	DescribeTable("GetWarningFormat",
		func(warningCode constants.WarningCode, expectedWarningFormat string) {
			Expect(error.GetWarningFormat(warningCode)).To(Equal(expectedWarningFormat))
		},
		Entry("unknown warning type", constants.WarningCode(998), `unknown warning`),
		Entry("nic speed too high", error.NicSpeedTooHigh, `vmnic "%s" of host "%s" speed is too high. Expected speed: %dMbps; actual speed: %dMbps.`),
		Entry("LLDP not found", error.LldpNotFound, `LLDP is not enabled on the vmnic "%s" on host "%s"`),
		Entry("two IP addresses required for mirrored", error.TwoExternalIpAddressesRequired, `please ensure that you have two external IP addresses for mirrored deployments`),
		Entry("one IP address required for mirrorless", error.OneExternalIpAddressRequired, `please ensure that you have one external IP address for mirrorless deployments`),
		Entry("extra IP addresses removed for mirrorless", error.ExtraIpAddressesRemoved, `removed %s from the list of gp-virtual-external IPs`),
		Entry("base VM IP reset to 0.0.0.0", error.BaseVmIpReset, `base VM IP set to 0.0.0.0`),
		Entry("base VM netmask reset to 0.0.0.0", error.BaseVmNetmaskReset, `base VM IP netmask set to 0.0.0.0`),
		Entry("base VM gateway IP reset to 0.0.0.0", error.BaseVmGatewayIpReset, `base VM gateway IP set to 0.0.0.0`),
		Entry("warn user to set base VM IP", error.BaseVmSetIp, `please set base VM IP`),
		Entry("warn user to set base VM netmask", error.BaseVmSetNetmask, `please set base VM IP netmask`),
		Entry("warn user to set base VM gateway IP", error.BaseVmSetGatewayIp, `please set base VM gateway IP`),
		Entry("base VM netmask reset to 0.0.0.0", error.BaseVmNetmaskReset, `base VM IP netmask set to 0.0.0.0`),
		Entry("IP for given network type cannot be changed", error.BaseVmIpWarningForDhcp, `IP for given network type cannot be changed`),
		Entry("Netmask for given network type cannot be changed", error.BaseVmNetmaskWarningForDhcp, `Netmask for given network type cannot be changed`),
		Entry("Gateway for given network type cannot be changed", error.BaseVmGatewayWarningForDhcp, `Gateway for given network type cannot be changed`),
		Entry("package already installed", error.PackageAlreadyInstalled, `%s already installed`),
	)
})
