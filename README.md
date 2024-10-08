# RESTIC_MON

Exposes restic backup status from s3 to monitoring tools.

## Environment Variables

```
S3_URL (required): URL to S3 restic repository
AWS_ACCESS_KEY_ID (required): Access key ID for S3
AWS_SECRET_ACCESS_KEY (required): Access key for S3
AWS_REGION (default us-east-1): S3 region
WARN_AGE_HOURS (default 36): Age of backup in hours for "warning" status
CRIT_AGE_HOURS (default 73): Age of backup in hours for "critical" status
BUCKET_PREFIX (default: none): Check only buckets with this prefix. The prefix is on status messages removed.
SEARCH_FOLDERS (default: false): Rather than expecting one backup per bucket, expect one backup per folder within buckets
BUCKET_NAMES (default: none): Use this buckets rather than scan for available buckets (comma or whitespace separated list)
```

### Notes

If `S3_URL` contains `{S3_REGION}`, it is replaced by the region of the current bucket and a client for that region is created. This is useful if `ListBuckets` returns buckets for multiple regions: to list the objects in a bucket, an s3 client for that bucket must be used.

Endpoints (at port 8080):

* /health: returns 200 OK
* /json: returns a JSON object with "status" (OK|WARNING|CRITICAL) and "message" with a detailed message, suitable to use with nagios
* /metrics: returns prometheus metrics for each backup:
  `restic_backup_count{name="BACKUP-NAME",bucket="BUCKET-NAME"} 123` - the number of existing restic snapshots
  `restic_backup_age_hours{name="BACKUP-NAME",bucket="BUCKET-NAME"} 8.25 ` - age of the latest backup in hours
