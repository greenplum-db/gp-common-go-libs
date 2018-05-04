#!/bin/bash

DIR="github.com/greenplum-db/gp-common-go-libs"
RESULTS="/tmp/results.out"
echo "mode: set" > /tmp/coverage.out # Need this line at the start of the file for the total coverage at the end
for PACKAGE in "cluster" "dbconn" "gplog" "iohelper" "structmatcher"; do
  # Generate code coverage statistics for all packages, write the coverage statistics to a file, and print the coverage percentage to the shell
  go test -coverpkg "$DIR/$PACKAGE" "$DIR/$PACKAGE" -coverprofile="/tmp/unit_$PACKAGE.out" | awk '{printf("%s unit test coverage|%s", $2, $5)}' | awk -F"/" '{print $4}' >> $RESULTS
  # Filter out the first "mode: set" line from each coverage file and concatenate them all
  cat "/tmp/unit_$PACKAGE.out" | awk '{if($1!="mode:") {print $1 " " $2 " " $3}}' >> /tmp/coverage.out
done

# Print the total coverage percentage and generate a coverage HTML page
go tool cover -func=/tmp/coverage.out | awk '{if($1=="total:") {printf("%s|%s\n", $1, $3)}}' >> $RESULTS
column -s"|" -t $RESULTS
rm $RESULTS
exit 0
