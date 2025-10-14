#! /bin/bash

cd ./src/sqlancer/go_parser
go run ./seed_refiner.go "$1" "$2"
