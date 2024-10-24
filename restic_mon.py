#!/usr/bin/env python3

import boto3
from datetime import datetime
import time
import os
import re
from socketserver import ThreadingMixIn
from http.server import HTTPServer,BaseHTTPRequestHandler
import json
import argparse


def get_env(name,default=None):
    if name in os.environ:
        return os.environ[name]
    if default is not None:
        return default
    print("Please set %s"%name)
    quit(1)

def get_s3_client(region=None):
    s3_url=get_env("S3_URL")
    aws_access_key_id=get_env("AWS_ACCESS_KEY_ID")
    aws_secret_access_key=get_env("AWS_SECRET_ACCESS_KEY")
    aws_region=region if region else get_env("AWS_REGION","us-east-1")

    # region and url must match
    s3_url = s3_url.replace("{S3_REGION}", aws_region)

    session = boto3.session.Session()
    s3=session.client(
            region_name=aws_region,
            service_name='s3',
            endpoint_url=s3_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
    )
    return s3

def get_backup_status(bucket_name,backup_name,folder_prefix="",s3=None):
    if s3 is None:
        s3=get_s3_client()

    backup = {
            "name": backup_name,
            "bucket": bucket_name,
            "time": None,
            "age_hours": None,
            "error": None,
            "count": 0,
    }

    try:
        for page in s3.get_paginator('list_objects').paginate(
                Bucket=bucket_name,
                Prefix=folder_prefix+'snapshots'):
            if 'Contents' in page:
                for item in page['Contents']:
                    last_modfied=item['LastModified']
                    backup['count']=backup['count']+1
                    if backup['time'] is None or backup['time']<last_modfied:
                        backup['time']=last_modfied
                        backup['age_hours']=(datetime.now(tz=last_modfied.tzinfo)-last_modfied).total_seconds()/3600
    except Exception as e:
        backup['error']=str(e)

    return backup


def find_bucket_names(bucket_prefix,s3):
    # search buckets
    result=[]
    buckets=s3.list_buckets()
    for bucket in buckets['Buckets']:
        bucket_name=bucket['Name']
        if bucket_name.startswith(bucket_prefix):
            result.append(bucket_name)
    return result

def find_backups(s3=None):

    if s3 is None:
        s3=get_s3_client()

    backups=[]

    bucket_prefix=get_env("BUCKET_PREFIX","")
    _bucket_names=get_env("BUCKET_NAMES","")
    if _bucket_names=="":
        bucket_names=find_bucket_names(bucket_prefix, s3)
    else:
        bucket_names=re.split(r"[,\s]+",_bucket_names)

    if get_env("SEARCH_FOLDERS","false")=="true":
        for bucket_name in bucket_names:

            # get new client if bucket region doesn't match region from current client
            bucket_region = s3.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]

            if bucket_region != s3.meta.region_name:
                s3 = get_s3_client(bucket_region)

            # search in objects within bucket
            for page in s3.get_paginator('list_objects').paginate(
                    Bucket=bucket_name,Delimiter='/'):
                if 'CommonPrefixes' in page:
                    for item in page['CommonPrefixes']:
                        folder_prefix=item['Prefix']
                        backup_name=folder_prefix[:-1]
                        backups.append(get_backup_status(bucket_name,backup_name,folder_prefix,s3))
    else:
        # search directly in bucket
        for bucket_name in bucket_names:
            if bucket_name.startswith(bucket_prefix):
                backup_name=bucket_name[len(bucket_prefix):]
            else:
                backup_name=bucket_name[len(bucket_prefix):]

            # get new client if bucket region doesn't match region from current client
            bucket_region = s3.get_bucket_location(Bucket=bucket_name)["LocationConstraint"]

            if bucket_region != s3.meta.region_name:
                s3 = get_s3_client(bucket_region)

            backups.append(get_backup_status(bucket_name,backup_name,"",s3))

    return backups

cached=None
cached_until=0
def find_backups_cached():
    global cached_until,cached
    if cached_until<time.time():
        cached=find_backups()
        # cache for 30s to avoid DoS
        cached_until=time.time()+30
    return cached

def get_backups_json(backups):
    try:
        ok=[]
        warn=[]
        crit=[]

        warn_age_hours=int(get_env("WARN_AGE_HOURS",36))
        crit_age_hours=int(get_env("CRIT_AGE_HOURS",72))

        for backup in backups:
            if backup['error']:
                crit.append("%s: %s"%(backup['name'],backup['error']))
            elif backup['age_hours'] is None:
                crit.append("%s (no backup)"%backup['name'])
            elif backup['age_hours']>crit_age_hours:
                crit.append("%s (%sh ago)"%(backup['name'],round(backup['age_hours'])))
            elif backup['age_hours']>warn_age_hours:
                warn.append("%s (%sh ago)"%(backup['name'],round(backup['age_hours'])))
            else:
                ok.append("%s (%sh ago)"%(backup['name'],round(backup['age_hours'])))

        status='OK'
        message=[]
        if len(crit)>0:
            status='CRITICAL'
            message.append("CRITICAL: %s"%(", ".join(crit)))
        if len(warn)>0:
            if status=='OK':
                status='WARNING'
            message.append("WARNING: %s"%(", ".join(warn)))
        if len(ok)>0:
            message.append("OK: %s"%(", ".join(ok)))

        return {
                "status": status,
                "message": " // ".join(message)
        }

    except Exception as e:
        return {
                "status": "CRITICAL",
                "message": "Unable to check backups: %s: %s"%(type(e).__name__,e)
        }

def get_backups_metrics(backups):
    metrics=[]
    for backup in backups:
        labels='{name="%s",bucket="%s"}'%(backup["name"],backup["bucket"])
        metrics.append("restic_backup_count%s %s"%(labels,backup["count"]))
        if backup["age_hours"] is not None:
            metrics.append("restic_backup_age_hours%s %s"%(labels,backup["age_hours"]))

    return "\n".join(metrics)


# Webserver: https://gist.github.com/gnilchee/246474141cbe588eb9fb
class ThreadingSimpleServer(ThreadingMixIn, HTTPServer):
    pass

cached_json=None
cached_json_until=0
cached_metrics=None
cached_metrics_until=0
class MonRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path=="/health":
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write("OK\n".encode())
            return

        if self.path=="/json":
            backups=find_backups_cached()
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(get_backups_json(backups),indent=2).encode())
            return

        if self.path=="/metrics":
            backups=find_backups_cached()
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(get_backups_metrics(backups).encode())
            return

        self.send_response(404)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write("404 Not found.\n".encode())

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action="store_true", help='Run a check and exit')
    parser.add_argument('--metrics', action="store_true", help='Print as metrics and exit')
    args = parser.parse_args()

    if (args.check):
        result=get_backups_json(find_backups())
        print(result['message'])
        return

    if (args.metrics):
        print(get_backups_metrics(find_backups()))
        return

    server = ThreadingSimpleServer(('0.0.0.0', 8080), MonRequestHandler)
    print("Starting webserver on port 8080")
    try:
        while 1:
            server.handle_request()
    except KeyboardInterrupt:
        print("\nShutting down server per users request.")

if __name__ == "__main__":
    main()
