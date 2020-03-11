#!/bin/bash
root=/tmp/cta-ingest

mc rb --force rgw/cta-dev
set -e
rm -rf $root/{disassemble,download,origin,reassemble,target}
mkdir -p $root/origin
date > $root/origin/_123456_scan.txt
dd if=/dev/urandom of=$root/origin/_run123456.fits bs=1M count=20

function section() {
    heading="$@"
    printf -- '\n%s------- %s%s\n' $(tput rev) "$heading" $(tput sgr0)
    $@
}

section python3 -m cta_ingest -v -b cta-dev adv refresh_origin $root/origin/
    tree --noreport $root/origin
    mc cat rgw/cta-dev/origin.json | jq
section python3 -m cta_ingest -v -b cta-dev adv disassemble $root/disassemble/
    tree --noreport $root/disassemble
    mc cat rgw/cta-dev/disassemble.json | jq
section python3 -m cta_ingest -v -b cta-dev adv upload
    mc ls -r rgw/cta-dev/
    mc cat rgw/cta-dev/upload.json | jq
section python3 -m cta_ingest -v -b cta-dev adv download $root/download
    tree --noreport $root/download
    mc cat rgw/cta-dev/download.json | jq
section python3 -m cta_ingest -v -b cta-dev adv reassemble $root/reassemble $root/target
    ls -lh $root/origin
    ls -lh $root/target
section python3 -m cta_ingest -v -b cta-dev adv refresh_target $root/target
    tree --noreport $root/target
    mc cat rgw/cta-dev/target.json | jq
