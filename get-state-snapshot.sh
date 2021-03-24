#!/bin/bash

endpoint=$1
bucket=$2

mc cat $endpoint/$bucket/origin.json | jq > 1-origin.json
mc cat $endpoint/$bucket/disassemble.json | jq > 2-disassemble.json
mc cat $endpoint/$bucket/upload.json | jq > 3-upload.json
mc cat $endpoint/$bucket/download.json | jq > 4-download.json
mc cat $endpoint/$bucket/target.json | jq > 5-target.json

