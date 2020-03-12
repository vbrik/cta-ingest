#!/bin/bash
root=/tmp/cta-ingest

mc rb --force rgw/cta-dev
set -e
rm -rf $root/{disassemble,download,origin,reassemble,target}
mkdir -p $root/origin $root/target
date > $root/origin/_123456_scan.txt
dd if=/dev/urandom of=$root/origin/_run123456.fits bs=1M count=20

function section() {
    heading="$@"
    printf -- '\n%s------- %s%s\n' $(tput rev) "$heading" $(tput sgr0)
    $@
}

section ./cta-ingest.py -v -b cta-dev refresh_origin $root/origin/
    tree --noreport $root/origin
    mc cat rgw/cta-dev/origin.json | jq
section ./cta-ingest.py -v -b cta-dev refresh_target $root/target
    tree --noreport $root/target
    mc cat rgw/cta-dev/target.json | jq

section ./cta-ingest.py -v -b cta-dev disassemble $root/disassemble/
    tree --noreport $root/disassemble
    mc cat rgw/cta-dev/disassemble.json | jq
section ./cta-ingest.py -v -b cta-dev upload
    mc ls -r rgw/cta-dev/
    mc cat rgw/cta-dev/upload.json | jq
section ./cta-ingest.py -v -b cta-dev download $root/download
    tree --noreport $root/download
    mc cat rgw/cta-dev/download.json | jq
section ./cta-ingest.py -v -b cta-dev reassemble $root/reassemble $root/target
    ls -lh $root/origin
    ls -lh $root/target
section ./cta-ingest.py -v -b cta-dev refresh_target $root/target
    tree --noreport $root/target
    mc cat rgw/cta-dev/target.json | jq
