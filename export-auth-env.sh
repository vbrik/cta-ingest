#!/bin/bash

export AWS_ACCESS_KEY_ID=$(radosgw-admin user info --uid=cta | jq -r '.keys | .[] | .access_key')
export AWS_SECRET_ACCESS_KEY=$(radosgw-admin user info --uid=cta | jq -r '.keys | .[] | .secret_key')
