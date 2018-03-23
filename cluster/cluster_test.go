package cluster_test

import (
	"database/sql/driver"
	"fmt"
	"os"
	"os/user"
	"testing"

	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "cluster tests")
}

var (
	connection *dbconn.DBConn
	mock       sqlmock.Sqlmock
	logfile    *gbytes.Buffer
)

func expectPathToExist(path string) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Fail(fmt.Sprintf("Expected %s to exist", path))
	}
}

var _ = BeforeSuite(func() {
	_, _, _, _, logfile = testhelper.SetupTestEnvironment()
})

var _ = BeforeEach(func() {
	connection, mock = testhelper.CreateAndConnectMockDB(1)
})

var _ = Describe("cluster/cluster tests", func() {
	masterSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	remoteSegTwo := cluster.SegConfig{ContentID: 2, Hostname: "remotehost2", DataDir: "/data/gpseg2"}
	var (
		testCluster  cluster.Cluster
		testExecutor *testhelper.TestExecutor
	)

	BeforeEach(func() {
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testhelper.TestExecutor{}
		testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor
	})
	Describe("ConstructSSHCommand", func() {
		It("constructs an ssh command", func() {
			cmd := cluster.ConstructSSHCommand("some-host", "ls")
			Expect(cmd).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@some-host", "ls"}))
		})
	})
	Describe("GetSegmentConfiguration", func() {
		header := []string{"contentid", "hostname", "datadir"}
		localSegOne := []driver.Value{"0", "localhost", "/data/gpseg0"}
		localSegTwo := []driver.Value{"1", "localhost", "/data/gpseg1"}
		remoteSegOne := []driver.Value{"2", "remotehost", "/data/gpseg2"}

		It("returns a configuration for a single-host, single-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := cluster.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(1))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
		})
		It("returns a configuration for a single-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := cluster.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
		})
		It("returns a configuration for a multi-host, multi-segment cluster", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(localSegOne...).AddRow(localSegTwo...).AddRow(remoteSegOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := cluster.GetSegmentConfiguration(connection)
			Expect(len(results)).To(Equal(3))
			Expect(results[0].DataDir).To(Equal("/data/gpseg0"))
			Expect(results[0].Hostname).To(Equal("localhost"))
			Expect(results[1].DataDir).To(Equal("/data/gpseg1"))
			Expect(results[1].Hostname).To(Equal("localhost"))
			Expect(results[2].DataDir).To(Equal("/data/gpseg2"))
			Expect(results[2].Hostname).To(Equal("remotehost"))
		})
	})
	Describe("GenerateSSHCommandMapForSegments", func() {
		It("Returns a map of ssh commands for the master, including master", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg})
			commandMap := testCluster.GenerateSSHCommandMapForSegments(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[-1]).To(Equal([]string{"bash", "-c", "ls"}))
		})
		It("Returns a map of ssh commands for the master, excluding master", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg})
			commandMap := testCluster.GenerateSSHCommandMapForSegments(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(0))
		})
		It("Returns a map of ssh commands for one segment, including master", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{remoteSegOne})
			commandMap := testCluster.GenerateSSHCommandMapForSegments(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "ls"}))
		})
		It("Returns a map of ssh commands for one segment, excluding master", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{remoteSegOne})
			commandMap := testCluster.GenerateSSHCommandMapForSegments(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "ls"}))
		})
		It("Returns a map of ssh commands for two segments on the same host, including master", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne})
			commandMap := testCluster.GenerateSSHCommandMapForSegments(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(2))
			Expect(commandMap[-1]).To(Equal([]string{"bash", "-c", "ls"}))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "ls"}))
		})
		It("Returns a map of ssh commands for two segments on the same host, excluding master", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne})
			commandMap := testCluster.GenerateSSHCommandMapForSegments(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "ls"}))
		})
		It("Returns a map of ssh commands for three segments on different hosts", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{localSegOne, remoteSegOne, remoteSegTwo})
			commandMap := testCluster.GenerateSSHCommandMapForSegments(false, func(contentID int) string {
				return fmt.Sprintf("echo %d", contentID)
			})
			Expect(len(commandMap)).To(Equal(3))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "echo 0"}))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "echo 1"}))
			Expect(commandMap[2]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost2", "echo 2"}))
		})
	})
	Describe("GenerateSSHCommandMapForHosts", func() {
		It("Returns a map of ssh commands for the master host, including the master host", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg})
			commandMap := testCluster.GenerateSSHCommandMapForHosts(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[-1]).To(Equal([]string{"bash", "-c", "ls"}))
		})
		It("Returns a map of ssh commands for the master host, excluding the master host", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg})
			commandMap := testCluster.GenerateSSHCommandMapForHosts(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(0))
		})
		It("Returns a map of ssh commands for one host, including the master host", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{remoteSegOne})
			commandMap := testCluster.GenerateSSHCommandMapForHosts(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "ls"}))
		})
		It("Returns a map of ssh commands for one host, excluding the master host", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{remoteSegOne})
			commandMap := testCluster.GenerateSSHCommandMapForHosts(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "ls"}))
		})
		It("Returns a map of ssh commands for one host containing two segments, including the master host", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne})
			commandMap := testCluster.GenerateSSHCommandMapForHosts(true, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			// Either -1 or 0 will be present, but which content isn't guaranteed since we only really care about the host
			if _, ok := commandMap[-1]; ok {
				Expect(commandMap[-1]).To(Equal([]string{"bash", "-c", "ls"}))
			} else {
				Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "ls"}))
			}
		})
		It("Returns a map of ssh commands for one host containing two segments, excluding the master host", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne})
			commandMap := testCluster.GenerateSSHCommandMapForHosts(false, func(_ int) string {
				return "ls"
			})
			Expect(len(commandMap)).To(Equal(1))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "ls"}))
		})
		It("Returns a map of ssh commands for one master host and two remote hosts, including the master host", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne, remoteSegTwo})
			commandMap := testCluster.GenerateSSHCommandMapForHosts(true, func(contentID int) string {
				return fmt.Sprintf("echo %d", contentID)
			})
			Expect(len(commandMap)).To(Equal(3))
			// Either -1 or 0 will be present, but which content isn't guaranteed since we only really care about the host
			if _, ok := commandMap[-1]; ok {
				Expect(commandMap[-1]).To(Equal([]string{"bash", "-c", "echo -1"}))
			} else {
				Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "echo 0"}))
			}
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "echo 1"}))
			Expect(commandMap[2]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost2", "echo 2"}))
		})
		It("Returns a map of ssh commands for one master host and two remote hosts, excluding the master host", func() {
			testCluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne, remoteSegTwo})
			commandMap := testCluster.GenerateSSHCommandMapForHosts(false, func(contentID int) string {
				return fmt.Sprintf("echo %d", contentID)
			})
			Expect(len(commandMap)).To(Equal(3))
			Expect(commandMap[0]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@localhost", "echo 0"}))
			Expect(commandMap[1]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost1", "echo 1"}))
			Expect(commandMap[2]).To(Equal([]string{"ssh", "-o", "StrictHostKeyChecking=no", "testUser@remotehost2", "echo 2"}))
		})
	})
	Describe("ExecuteLocalCommand", func() {
		BeforeEach(func() {
			os.MkdirAll("/tmp/gp_common_go_libs_test", 0777)
		})
		AfterEach(func() {
			os.RemoveAll("/tmp/gp_common_go_libs_test")
		})
		It("runs the specified command", func() {
			testCluster := cluster.Cluster{}
			commandStr := "touch /tmp/gp_common_go_libs_test/foo"
			testCluster.Executor = &cluster.GPDBExecutor{}
			testCluster.ExecuteLocalCommand(commandStr)

			expectPathToExist("/tmp/gp_common_go_libs_test/foo")
		})
		It("returns any error generated by the specified command", func() {
			testCluster := cluster.Cluster{}
			commandStr := "some-non-existent-command /tmp/gp_common_go_libs_test/foo"
			testCluster.Executor = &cluster.GPDBExecutor{}
			output, err := testCluster.ExecuteLocalCommand(commandStr)

			Expect(output).To(Equal("bash: some-non-existent-command: command not found\n"))
			Expect(err.Error()).To(Equal("exit status 127"))
		})
	})
	Describe("ExecuteClusterCommand", func() {
		BeforeEach(func() {
			os.MkdirAll("/tmp/gp_common_go_libs_test", 0777)
		})
		AfterEach(func() {
			os.RemoveAll("/tmp/gp_common_go_libs_test")
		})
		It("runs commands specified by command map", func() {
			testCluster := cluster.Cluster{}
			commandMap := map[int][]string{
				-1: {"touch", "/tmp/gp_common_go_libs_test/foo"},
				0:  {"touch", "/tmp/gp_common_go_libs_test/baz"},
			}
			testCluster.Executor = &cluster.GPDBExecutor{}
			testCluster.ExecuteClusterCommand(cluster.ON_SEGMENTS_AND_MASTER, commandMap)

			expectPathToExist("/tmp/gp_common_go_libs_test/foo")
			expectPathToExist("/tmp/gp_common_go_libs_test/baz")
		})
		It("returns any errors generated by any of the commands", func() {
			testCluster := cluster.Cluster{}
			commandMap := map[int][]string{
				-1: {"touch", "/tmp/gp_common_go_libs_test/foo"},
				0:  {"some-non-existent-command"},
			}
			testCluster.Executor = &cluster.GPDBExecutor{}
			clusterOutput := testCluster.ExecuteClusterCommand(cluster.ON_SEGMENTS_AND_MASTER, commandMap)

			expectPathToExist("/tmp/gp_common_go_libs_test/foo")
			Expect(clusterOutput.NumErrors).To(Equal(1))
			Expect(clusterOutput.Errors[0].Error()).To(Equal("exec: \"some-non-existent-command\": executable file not found in $PATH"))
		})
	})
	Describe("CheckClusterError", func() {
		It("prints error messages for a command executed on a per-segment basis", func() {
			remoteOutput := &cluster.RemoteOutput{
				Scope:     cluster.ON_SEGMENTS,
				NumErrors: 1,
				Stderrs: map[int]string{
					1: "exit status 1",
				},
				Errors: map[int]error{
					1: errors.Errorf("ssh error"),
				},
				CmdStrs: map[int]string{
					1: "this is the command",
				},
			}
			defer testhelper.ShouldPanicWithMessage("Got an error on 1 segment. See gbytes.Buffer for a complete list of errors.")
			defer Expect(logfile).To(gbytes.Say(`\[DEBUG\]:-Error received on segment 1 on host remotehost1 with error ssh error: exit status 1 Command was: this is the command`))
			testCluster.CheckClusterError(remoteOutput, "Got an error", func(contentID int) string {
				return "Error received"
			})
		})
		It("prints error messages for a command executed on a per-host basis", func() {
			remoteOutput := &cluster.RemoteOutput{
				Scope:     cluster.ON_HOSTS,
				NumErrors: 1,
				Stderrs: map[int]string{
					1: "exit status 1",
				},
				Errors: map[int]error{
					1: errors.Errorf("ssh error"),
				},
			}
			defer testhelper.ShouldPanicWithMessage("Got an error on 1 host. See gbytes.Buffer for a complete list of errors.")
			defer Expect(logfile).To(gbytes.Say(`\[DEBUG\]:-Error received on host remotehost1 with error ssh error: exit status 1`))
			testCluster.CheckClusterError(remoteOutput, "Got an error", func(contentID int) string {
				return "Error received"
			})
		})
	})
	Describe("LogFatalClusterError", func() {
		It("logs an error for 1 segment", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 1 segment. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_SEGMENTS_AND_MASTER, 1)
		})
		It("logs an error for more than 1 segment", func() {
			defer testhelper.ShouldPanicWithMessage("Error occurred on 2 segments. See gbytes.Buffer for a complete list of errors.")
			cluster.LogFatalClusterError("Error occurred", cluster.ON_SEGMENTS_AND_MASTER, 2)
		})
	})
	Describe("cluster setup and accessor functions", func() {
		masterSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
		localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
		localSegTwo := cluster.SegConfig{ContentID: 1, Hostname: "localhost", DataDir: "/data/gpseg1"}
		remoteSegTwo := cluster.SegConfig{ContentID: 1, Hostname: "remotehost", DataDir: "/data/gpseg1"}
		It("returns content dir for a single-host, single-segment nodes", func() {
			cluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne})
			Expect(len(cluster.GetContentList())).To(Equal(2))
			Expect(cluster.SegDirMap[-1]).To(Equal("/data/gpseg-1"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.SegDirMap[0]).To(Equal("/data/gpseg0"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
		})
		It("sets up the configuration for a single-host, multi-segment cluster", func() {
			cluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, localSegTwo})
			Expect(len(cluster.GetContentList())).To(Equal(3))
			Expect(cluster.SegDirMap[-1]).To(Equal("/data/gpseg-1"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.SegDirMap[0]).To(Equal("/data/gpseg0"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(cluster.SegDirMap[1]).To(Equal("/data/gpseg1"))
			Expect(cluster.GetHostForContent(1)).To(Equal("localhost"))
		})
		It("sets up the configuration for a multi-host, multi-segment cluster", func() {
			cluster := cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegTwo})
			Expect(len(cluster.GetContentList())).To(Equal(3))
			Expect(cluster.SegDirMap[-1]).To(Equal("/data/gpseg-1"))
			Expect(cluster.GetHostForContent(-1)).To(Equal("localhost"))
			Expect(cluster.SegDirMap[0]).To(Equal("/data/gpseg0"))
			Expect(cluster.GetHostForContent(0)).To(Equal("localhost"))
			Expect(cluster.SegDirMap[1]).To(Equal("/data/gpseg1"))
			Expect(cluster.GetHostForContent(1)).To(Equal("remotehost"))
		})
	})
})
