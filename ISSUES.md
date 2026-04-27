# Files at origin must exist until they reach the destination
We ought to define what should happen if "in-flight" file is deleted at the origin.
Currently, this situation is not guaranteed to be handled gracefully.
Some stages handle "orphan" files, but, notably, reassemble needs file metadata from the origin and will not be able to deliver files that disappear from origin.
Fixing this wouldn't be difficult, but may require pervasive changes, depending on what we decide the right approach is.

# In Popen pipelines, failure of the first commands in not detected

# Incomplete multipart uploads not cleaned-up
If the upload to S3 process is interrupted the multipart upload is left in-flight in the RGW bucket. These incomplete multipart uploads:
- Are invisible as regular objects (won't appear in list_keys)
- Still consume storage on the Ceph cluster
- Persist indefinitely until explicitly aborted via abort_multipart_upload

try/except around upload_file that would call s3c.abort_multipart_upload(...) would be good enough fix? (will it catch the alarm signal?)

# Directory transfers not supported
