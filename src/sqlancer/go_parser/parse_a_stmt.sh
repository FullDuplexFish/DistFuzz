#! /bin/bash

cd ./src/sqlancer/go_parser
go run ./parse_a_stmt.go -dbname=$1 -original-stmt="$2"
