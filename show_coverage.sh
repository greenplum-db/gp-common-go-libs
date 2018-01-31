#!/bin/bash

RESULTS="/tmp/results.out"
go test -coverprofile=/tmp/coverage.out 2> /dev/null | awk '{print $2 " test " $4 "\t" $5}' | awk -F"/" '{print $4}' > $RESULTS

# Print the total coverage percentage and generate a coverage HTML page
go tool cover -func=/tmp/coverage.out | awk '{if($1=="total:") {print $1 "\t\t\t\t" $3}}' >> $RESULTS
cat $RESULTS
rm $RESULTS
exit 0
