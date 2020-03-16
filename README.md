# CTA Ingest
This application transfers data between sites via an S3 bucket. It is intended for an environment with unusual networking restrictions.

"CTA" refers to the Cherenkov Telescope Array Observatory.

# Overview
CTA Ingest is like a highly-specialized rsync.

It operates roughly as follows:

On the source host:
1. Build a list of files in the source data directory ("origin").
1. Identify files that haven't yet been delivered to the final destination directory ("target").
1. Compress, split into parts, and upload those files to an S3 bucket.

On the destination host:
1. Download file parts from the bucket.
1. Concatenate and decompress them in a temporary location on the same file system as the target.
1. Move the file to the target directory (moving ensures no partial files in the target).

Run the application with the `--help` flag for more information.

# Installation
CTA Ingest requires Python3 and [zstd](https://facebook.github.io/zstd/) (e.g. `yum install -y zstd`).

    git clone https://github.com/vbrik/cta-ingest.git
    cd cta-ingest
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt

## AWS Authentication
If AWS credentials are not supplied as command-line arguments, CTA Ingest will rely on [boto3](https://boto3.readthedocs.io) to determine them. At the time of writing, this meant environmental variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, or file `~/.aws/config`, which could look like this:
```
[default]
aws_access_key_id = XXX
aws_secret_access_key = YYY
```
