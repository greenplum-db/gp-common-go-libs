package cluster

/*
 * This file contains structs and functions related to interacting
 * with files and directories, both locally and remotely over SSH.
 */

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/pkg/errors"
)

type Executor interface {
	ExecuteLocalCommand(commandStr string) (string, error)
	ExecuteClusterCommand(scope int, commandMap map[int][]string) *RemoteOutput
}

// This type only exists to allow us to mock Execute[...]Command functions for testing
type GPDBExecutor struct{}

type Cluster struct {
	ContentIDs []int
	SegDirMap  map[int]string
	SegHostMap map[int]string
	Executor
}

type SegConfig struct {
	ContentID int
	Hostname  string
	DataDir   string
}

/*
 * We pass values from this enum into GenerateAndExecuteCommand to define the
 * scope for remote command execution.
 * - ON_SEGMENTS: Execute on each segment, excluding the master.
 * - ON_SEGMENTS_AND_MASTER: Execute on each segment, including the master.
 * - ON_HOSTS: Execute on each host, excluding the master host.
 * - ON_HOSTS_AND_MASTER: Execute on each host, including the master host.
 */
const (
	ON_SEGMENTS = iota
	ON_SEGMENTS_AND_MASTER
	ON_HOSTS
	ON_HOSTS_AND_MASTER
)

type RemoteOutput struct {
	Scope     int
	NumErrors int
	Stdouts   map[int]string
	Stderrs   map[int]string
	Errors    map[int]error
	CmdStrs   map[int]string
}

/*
 * Base cluster functions
 */

func NewCluster(segConfigs []SegConfig) Cluster {
	cluster := Cluster{}
	cluster.SegHostMap = make(map[int]string, 0)
	cluster.SegDirMap = make(map[int]string, 0)
	for _, seg := range segConfigs {
		cluster.ContentIDs = append(cluster.ContentIDs, seg.ContentID)
		cluster.SegDirMap[seg.ContentID] = seg.DataDir
		cluster.SegHostMap[seg.ContentID] = seg.Hostname
	}
	cluster.Executor = &GPDBExecutor{}
	return cluster
}

func (cluster *Cluster) GenerateSegmentSSHCommand(contentID int, generateCommand func(int) string) []string {
	cmdStr := generateCommand(contentID)
	if contentID == -1 {
		return []string{"bash", "-c", cmdStr}
	}
	return ConstructSSHCommand(cluster.GetHostForContent(contentID), cmdStr)
}

func (cluster *Cluster) GenerateSSHCommandMapForSegments(includeMaster bool, generateCommand func(int) string) map[int][]string {
	commandMap := make(map[int][]string, len(cluster.ContentIDs))
	for _, contentID := range cluster.ContentIDs {
		if contentID == -1 && !includeMaster {
			continue
		}
		commandMap[contentID] = cluster.GenerateSegmentSSHCommand(contentID, generateCommand)
	}
	return commandMap
}

func (cluster *Cluster) GenerateSSHCommandMapForHosts(includeMaster bool, generateCommand func(int) string) map[int][]string {
	/*
	 * Derive a list of unique hosts from hostSegMap and then generate commands
	 * for each.  If includeMaster is false but there are segments on the master
	 * host, such as for a single-node cluster, the master host will be included.
	 */
	hostSegMap := make(map[string]int, 0)
	for contentID, host := range cluster.SegHostMap {
		if contentID == -1 && !includeMaster {
			continue
		}
		hostSegMap[host] = contentID
	}
	commands := make(map[int][]string, 0)
	for _, contentID := range hostSegMap {
		commands[contentID] = cluster.GenerateSegmentSSHCommand(contentID, generateCommand)
	}
	return commands
}

func (executor *GPDBExecutor) ExecuteLocalCommand(commandStr string) (string, error) {
	output, err := exec.Command("bash", "-c", commandStr).CombinedOutput()
	return string(output), err
}

func newRemoteOutput(scope int, numIDs int) *RemoteOutput {
	stdout := make(map[int]string, numIDs)
	stderr := make(map[int]string, numIDs)
	err := make(map[int]error, numIDs)
	cmdStr := make(map[int]string, numIDs)
	return &RemoteOutput{Scope: scope, NumErrors: 0, Stdouts: stdout, Stderrs: stderr, Errors: err, CmdStrs: cmdStr}
}

func (executor *GPDBExecutor) ExecuteClusterCommand(scope int, commandMap map[int][]string) *RemoteOutput {
	length := len(commandMap)
	finished := make(chan int)
	contentIDs := make([]int, length)
	i := 0
	for key := range commandMap {
		contentIDs[i] = key
		i++
	}
	output := newRemoteOutput(scope, length)
	stdouts := make([]string, length)
	stderrs := make([]string, length)
	errors := make([]error, length)
	for i, contentID := range contentIDs {
		go func(index int, segCommand []string) {
			var stderr bytes.Buffer
			cmd := exec.Command(segCommand[0], segCommand[1:]...)
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			stdouts[index] = string(out)
			stderrs[index] = stderr.String()
			errors[index] = err
			finished <- index
		}(i, commandMap[contentID])
	}
	for i := 0; i < length; i++ {
		index := <-finished
		id := contentIDs[index]
		output.Stdouts[id] = stdouts[index]
		output.Stderrs[id] = stderrs[index]
		output.Errors[id] = errors[index]
		output.CmdStrs[id] = strings.Join(commandMap[id], " ")
		if output.Errors[id] != nil {
			output.NumErrors++
		}
	}
	return output
}

/*
 * GenerateAndExecuteCommand and CheckClusterError are generic wrapper functions
 * to simplify execution of shell commands on remote hosts.
 */
func (cluster *Cluster) GenerateAndExecuteCommand(verboseMsg string, execFunc func(contentID int) string, scope int) *RemoteOutput {
	gplog.Verbose(verboseMsg)
	var commandMap map[int][]string
	switch scope {
	case ON_SEGMENTS:
		commandMap = cluster.GenerateSSHCommandMapForSegments(false, execFunc)
	case ON_SEGMENTS_AND_MASTER:
		commandMap = cluster.GenerateSSHCommandMapForSegments(true, execFunc)
	case ON_HOSTS:
		commandMap = cluster.GenerateSSHCommandMapForHosts(false, execFunc)
	case ON_HOSTS_AND_MASTER:
		commandMap = cluster.GenerateSSHCommandMapForHosts(true, execFunc)
	default:
		// If we ever get to this case, it's programmer error, not user error.
		gplog.Fatal(fmt.Errorf("Invalid remote execution scope for command to %s: %d", strings.ToLower(verboseMsg), scope), "")
	}

	return cluster.ExecuteClusterCommand(scope, commandMap)
}

func (cluster *Cluster) CheckClusterError(remoteOutput *RemoteOutput, finalErrMsg string, messageFunc func(contentID int) string, noFatal ...bool) {
	if remoteOutput.NumErrors == 0 {
		return
	}

	for contentID, err := range remoteOutput.Errors {
		if err != nil {
			segMsg := ""
			if remoteOutput.Scope != ON_HOSTS {
				segMsg = fmt.Sprintf("on segment %d ", contentID)
			}
			gplog.Verbose("%s %son host %s with error %s: %s Command was: %s", messageFunc(contentID), segMsg, cluster.GetHostForContent(contentID), err, remoteOutput.Stderrs[contentID], remoteOutput.CmdStrs[contentID])
		}
	}
	if len(noFatal) == 1 && noFatal[0] == true {
		gplog.Error(finalErrMsg)
	} else {
		LogFatalClusterError(finalErrMsg, remoteOutput.Scope, remoteOutput.NumErrors)
	}
}

func LogFatalClusterError(errMessage string, scope int, numErrors int) {
	segMsg := "segment"
	if scope == ON_HOSTS {
		segMsg = "host"
	}
	if numErrors != 1 {
		segMsg += "s"
	}
	gplog.Fatal(errors.Errorf("%s on %d %s. See %s for a complete list of errors.", errMessage, numErrors, segMsg, gplog.GetLogFilePath()), "")
}

func (cluster *Cluster) GetContentList() []int {
	return cluster.ContentIDs
}

func (cluster *Cluster) GetHostForContent(contentID int) string {
	return cluster.SegHostMap[contentID]
}

/*
 * Helper functions
 */

func GetSegmentConfiguration(connection *dbconn.DBConn) []SegConfig {
	query := ""
	if connection.Version.Before("6") {
		query = `
SELECT
	s.content as contentid,
	s.hostname,
	e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE s.role = 'p' AND f.fsname = 'pg_system'
ORDER BY s.content;`
	} else {
		query = `
SELECT
	content as contentid,
	hostname,
	datadir
FROM gp_segment_configuration
WHERE role = 'p'
ORDER BY content;`
	}

	results := make([]SegConfig, 0)
	err := connection.Select(&results, query)
	gplog.FatalOnError(err)
	return results
}

func ConstructSSHCommand(host string, cmd string) []string {
	currentUser, _ := operating.System.CurrentUser()
	user := currentUser.Username
	return []string{"ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", user, host), cmd}
}
