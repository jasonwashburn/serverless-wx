import json


def lambda_handler(event, context):
    # parse the SNS message to get the S3 event information
    s3_event = json.loads(event["Records"][0]["Sns"]["Message"])

    # get the S3 bucket and key from the event
    bucket = s3_event["Records"][0]["s3"]["bucket"]["name"]
    key = s3_event["Records"][0]["s3"]["object"]["key"]
    if "atmos/gfs.t" in key and "pgrb2.0p25" in key and ".idx" not in key:
        print(f"QUEUEING - key: {key}")
    else:
        print(f"SKIPPING - unknown filetype: {key}")
    return bucket, key
