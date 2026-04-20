# In Popen pipelines, failure of the first commands in not detected

# Incomplete multipart uploads not cleaned-up
If the upload to S3 process is interrupted the multipart upload is left in-flight in the RGW bucket. These incomplete multipart uploads:
- Are invisible as regular objects (won't appear in list_keys)
- Still consume storage on the Ceph cluster
- Persist indefinitely until explicitly aborted via abort_multipart_upload

try/except around upload_file that would call s3c.abort_multipart_upload(...) would be good enough fix? (will it catch the alarm signal?)

# Directory transfers not supported
