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
	ExecuteClusterCommand(scope int, commandList []ShellCommand) *RemoteOutput
}

// This type only exists to allow us to mock Execute[...]Command functions for testing
type GPDBExecutor struct{}

/*
 * A Cluster object stores information about the cluster in three ways:
 * - Segments is basically equivalent to gp_segment_configuration, a plain
 *   list of segment information, and is ordered by content id.
 * - ByContent is a map of content id to the single corresponding segment.
 * - ByHost is a map of hostname to all of the segments on that host.
 * The maps are only stored for efficient lookup; Segments is the "source of
 * truth" for the cluster.  The maps actually hold pointers to the SegConfigs
 * in Segments, so modifying Segments will modify the maps as well.
 */
type Cluster struct {
	ContentIDs []int
	Hostnames  []string
	Segments   []SegConfig
	ByContent  map[int]*SegConfig
	ByHost     map[string][]*SegConfig
	Executor
}

type SegConfig struct {
	DbID      int
	ContentID int
	Port      int
	Hostname  string
	DataDir   string
}

/*
 * We pass values from this enum into ShellCommands, RemoteOutputs, and associated
 * functions to define the nature and scope of the command execution.
 * - ON_SEGMENTS:            Execute on each segment, excluding the master.
 * - ON_SEGMENTS_AND_MASTER: Execute on each segment, including the master.
 * - ON_HOSTS:               Execute on each host, excluding the master host.
 * - ON_HOSTS_AND_MASTER:    Execute on each host, including the master host.
 *
 * - ON_MASTER_TO_SEGMENTS:            Execute commands on master about segments, excluding master.
 * - ON_MASTER_TO_SEGMENTS_AND_MASTER: Execute commands on master about segments, including master.
 * - ON_MASTER_TO_HOSTS:               Execute commands on master about hosts, excluding master.
 * - ON_MASTER_TO_HOSTS_AND_MASTER:    Execute commands on master about hosts, including master.
 */
const (
	ON_SEGMENTS = iota
	ON_SEGMENTS_AND_MASTER
	ON_HOSTS
	ON_HOSTS_AND_MASTER

	ON_MASTER_TO_SEGMENTS
	ON_MASTER_TO_SEGMENTS_AND_MASTER
	ON_MASTER_TO_HOSTS
	ON_MASTER_TO_HOSTS_AND_MASTER
)

func scopeIsRemote(scope int) bool {
	return scope == ON_SEGMENTS || scope == ON_SEGMENTS_AND_MASTER || scope == ON_HOSTS || scope == ON_HOSTS_AND_MASTER
}

func scopeIncludesMaster(scope int) bool {
	return scope == ON_SEGMENTS_AND_MASTER || scope == ON_MASTER_TO_SEGMENTS_AND_MASTER || scope == ON_HOSTS_AND_MASTER || scope == ON_MASTER_TO_HOSTS_AND_MASTER
}

func scopeIsHosts(scope int) bool {
	return scope == ON_HOSTS || scope == ON_HOSTS_AND_MASTER || scope == ON_MASTER_TO_HOSTS || scope == ON_MASTER_TO_HOSTS_AND_MASTER
}

/*
 * A ShellCommand stores a command to be executed (in both executable and
 * display form), as well as the results of the command execution and the
 * necessary information to determine how the command will be or was executed.
 *
 * It is assumed that before a caller references Content or Host for a given
 * command, they will check Scope to ensure that that field is meaningful for
 * that command.  GenerateCommandList sets Host to "" for per-segment commands
 * and Content to -2 for per-host commands, just to be safe.
 */
type ShellCommand struct {
	Scope         int
	Content       int
	Host          string
	Command       *exec.Cmd
	CommandString string
	Stdout        string
	Stderr        string
	Error         error
}

func NewShellCommand(scope int, content int, host string, command []string) ShellCommand {
	return ShellCommand{
		Scope:         scope,
		Content:       content,
		Host:          host,
		Command:       exec.Command(command[0], command[1:]...),
		CommandString: strings.Join(command, " "),
	}
}

/*
 * A RemoteOutput is used to make it easier to identify the success or failure
 * of a cluster command and to display the results to the user.
 */
type RemoteOutput struct {
	Scope          int
	NumErrors      int
	Commands       []ShellCommand
	FailedCommands []*ShellCommand
}

func NewRemoteOutput(scope int, numErrors int, commands []ShellCommand) *RemoteOutput {
	failedCommands := make([]*ShellCommand, numErrors)
	index := 0
	for i := range commands {
		if commands[i].Error != nil {
			failedCommands[index] = &commands[i]
			index++
		}
	}
	return &RemoteOutput{
		Scope:          scope,
		NumErrors:      numErrors,
		Commands:       commands,
		FailedCommands: failedCommands,
	}
}

/*
 * Base cluster functions
 */

func NewCluster(segConfigs []SegConfig) *Cluster {
	cluster := Cluster{}
	cluster.Segments = segConfigs
	cluster.ByContent = make(map[int]*SegConfig, len(segConfigs))
	cluster.ByHost = make(map[string][]*SegConfig, 0)
	for i := range cluster.Segments {
		segment := &cluster.Segments[i]
		cluster.ContentIDs = append(cluster.ContentIDs, segment.ContentID)
		cluster.ByContent[segment.ContentID] = segment
		cluster.ByHost[segment.Hostname] = append(cluster.ByHost[segment.Hostname], segment)
		if len(cluster.ByHost[segment.Hostname]) == 1 { // Only add each hostname once
			cluster.Hostnames = append(cluster.Hostnames, segment.Hostname)
		}
	}
	return &cluster
}

/*
 * Because cluster commands can be executed either per-segment or per-host, the
 * "generator" argument to this function can accept one of two types:
 * - func(int) []string, which takes a content id, for per-segment commands
 * - func(string) []string, which takes a hostname, for per-host commands
 * The function uses a type switch to identify the right one, and panics if
 * an invalid function type is passed in via programmer error.
 * This method makes it easier for the user to pass in whichever function fits
 * the kind of command they're generating, as opposed to having to pass in both
 * content and hostname regardless of scope or using some sort of helper struct.
 */
func (cluster *Cluster) GenerateCommandList(scope int, generator interface{}) []ShellCommand {
	var commands []ShellCommand
	switch generateCommand := generator.(type) {
	case func(content int) []string:
		for _, content := range cluster.ContentIDs {
			if content == -1 && !scopeIncludesMaster(scope) {
				continue
			}
			commands = append(commands, NewShellCommand(scope, content, "", generateCommand(content)))
		}
	case func(host string) []string:
		for _, host := range cluster.Hostnames {
			if host == cluster.GetHostForContent(-1) && !scopeIncludesMaster(scope) &&
				len(cluster.GetContentsForHost(host)) == 1 { // Only exclude the master host if there are no local segments
				continue
			}
			commands = append(commands, NewShellCommand(scope, -2, host, generateCommand(host)))
		}
	default:
		gplog.Fatal(nil, "Generator function passed to GenerateCommandList had an invalid function header.")
	}
	return commands
}

func ConstructSSHCommand(useLocal bool, host string, cmd string) []string {
	if useLocal {
		return []string{"bash", "-c", cmd}
	}
	currentUser, _ := operating.System.CurrentUser()
	user := currentUser.Username
	return []string{"ssh", "-o", "StrictHostKeyChecking=no", fmt.Sprintf("%s@%s", user, host), cmd}
}

/*
 * This function essentially wraps GenerateCommandList such that commands to be
 * executed on other hosts are sent through SSH and local commands use Bash.
 */
func (cluster *Cluster) GenerateSSHCommandList(scope int, generator interface{}) []ShellCommand {
	var commands []ShellCommand
	localHost := cluster.GetHostForContent(-1)
	switch generateCommand := generator.(type) {
	case func(content int) string:
		commands = cluster.GenerateCommandList(scope, func(content int) []string {
			useLocal := (cluster.GetHostForContent(content) == localHost || !scopeIsRemote(scope))
			cmd := generateCommand(content)
			return ConstructSSHCommand(useLocal, cluster.GetHostForContent(content), cmd)
		})
	case func(host string) string:
		commands = cluster.GenerateCommandList(scope, func(host string) []string {
			useLocal := (host == localHost || !scopeIsRemote(scope))
			cmd := generateCommand(host)
			return ConstructSSHCommand(useLocal, host, cmd)
		})
	}
	return commands
}

func (executor *GPDBExecutor) ExecuteLocalCommand(commandStr string) (string, error) {
	output, err := exec.Command("bash", "-c", commandStr).CombinedOutput()
	return string(output), err
}

/*
 * This function just executes all of the commands passed to it in parallel; it
 * doesn't care about the scope of the command except to pass that on to the
 * RemoteOutput after execution.
 * TODO: Add batching to prevent bottlenecks when executing in a huge cluster.
 */
func (executor *GPDBExecutor) ExecuteClusterCommand(scope int, commandList []ShellCommand) *RemoteOutput {
	length := len(commandList)
	finished := make(chan int)
	numErrors := 0
	for i := range commandList {
		go func(index int) {
			command := commandList[index]
			var stderr bytes.Buffer
			cmd := command.Command
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			command.Stdout = string(out)
			command.Stderr = stderr.String()
			command.Error = err
			commandList[index] = command
			finished <- index
		}(i)
	}
	for i := 0; i < length; i++ {
		index := <-finished
		if commandList[index].Error != nil {
			numErrors++
		}
	}
	return NewRemoteOutput(scope, numErrors, commandList)
}

/*
 * GenerateAndExecuteCommand and CheckClusterError are generic wrapper functions
 * to simplify execution of...
 * 1. shell commands directly on remote hosts via ssh.
 *    - e.g. running an ls on all hosts
 * 2. shell commands on master to push to remote hosts.
 *    - e.g. running multiple scps on master to push a file to all segments
 */
func (cluster *Cluster) GenerateAndExecuteCommand(verboseMsg string, scope int, generator interface{}) *RemoteOutput {
	gplog.Verbose(verboseMsg)
	commandList := cluster.GenerateSSHCommandList(scope, generator)
	return cluster.ExecuteClusterCommand(scope, commandList)
}

func (cluster *Cluster) CheckClusterError(remoteOutput *RemoteOutput, finalErrMsg string, messageFunc interface{}, noFatal ...bool) {
	if remoteOutput.NumErrors == 0 {
		return
	}
	for _, failedCommand := range remoteOutput.FailedCommands {
		errStr := fmt.Sprintf("with error %s: %s", failedCommand.Error, failedCommand.Stderr)
		switch getMessage := messageFunc.(type) {
		case func(content int) string:
			content := failedCommand.Content
			host := cluster.GetHostForContent(content)
			gplog.Verbose("%s on segment %d on host %s %s", getMessage(content), content, host, errStr)
		case func(host string) string:
			host := failedCommand.Host
			gplog.Verbose("%s on host %s %s", getMessage(host), host, errStr)
		}
		gplog.Verbose("Command was: %s", failedCommand.CommandString)
	}

	if len(noFatal) == 1 && noFatal[0] == true {
		gplog.Error(finalErrMsg)
	} else {
		LogFatalClusterError(finalErrMsg, remoteOutput.Scope, remoteOutput.NumErrors)
	}
}

func LogFatalClusterError(errMessage string, scope int, numErrors int) {
	str := " on"
	if !scopeIsRemote(scope) {
		str += " master for"
	}
	errMessage += str

	segMsg := "segment"
	if scopeIsHosts(scope) {
		segMsg = "host"
	}
	if numErrors != 1 {
		segMsg += "s"
	}
	gplog.Fatal(errors.Errorf("%s %d %s. See %s for a complete list of errors.", errMessage, numErrors, segMsg, gplog.GetLogFilePath()), "")
}

func (cluster *Cluster) GetDbidForContent(contentID int) int {
	return cluster.ByContent[contentID].DbID
}

func (cluster *Cluster) GetPortForContent(contentID int) int {
	return cluster.ByContent[contentID].Port
}

func (cluster *Cluster) GetHostForContent(contentID int) string {
	return cluster.ByContent[contentID].Hostname
}

func (cluster *Cluster) GetDirForContent(contentID int) string {
	return cluster.ByContent[contentID].DataDir
}

func (cluster *Cluster) GetDbidsForHost(hostname string) []int {
	dbids := make([]int, len(cluster.ByHost[hostname]))
	for i, seg := range cluster.ByHost[hostname] {
		dbids[i] = seg.DbID
	}
	return dbids
}

func (cluster *Cluster) GetContentsForHost(hostname string) []int {
	contents := make([]int, len(cluster.ByHost[hostname]))
	for i, seg := range cluster.ByHost[hostname] {
		contents[i] = seg.ContentID
	}
	return contents
}

func (cluster *Cluster) GetPortsForHost(hostname string) []int {
	ports := make([]int, len(cluster.ByHost[hostname]))
	for i, seg := range cluster.ByHost[hostname] {
		ports[i] = seg.Port
	}
	return ports
}

func (cluster *Cluster) GetDirsForHost(hostname string) []string {
	dirs := make([]string, len(cluster.ByHost[hostname]))
	for i, seg := range cluster.ByHost[hostname] {
		dirs[i] = seg.DataDir
	}
	return dirs
}

/*
 * Helper functions
 */

func GetSegmentConfiguration(connection *dbconn.DBConn) ([]SegConfig, error) {
	query := ""
	if connection.Version.Before("6") {
		query = `
SELECT
	s.dbid,
	s.content as contentid,
	s.port,
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
	dbid,
	content as contentid,
	port,
	hostname,
	datadir
FROM gp_segment_configuration
WHERE role = 'p'
ORDER BY content;`
	}

	results := make([]SegConfig, 0)
	err := connection.Select(&results, query)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func MustGetSegmentConfiguration(connection *dbconn.DBConn) []SegConfig {
	segConfigs, err := GetSegmentConfiguration(connection)
	gplog.FatalOnError(err)
	return segConfigs
}
