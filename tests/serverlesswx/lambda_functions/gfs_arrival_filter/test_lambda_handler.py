import pytest

from serverlesswx.lambda_functions.gfs_arrival_filter.lambda_function import \
    lambda_handler


@pytest.fixture
def new_gfs_grib_event():
    message = {
        "Records": [
            {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:us-east-1:123901341784:NewGFSObject:99e6fed7-5577-4bcc-a06d-3eb9101cbe66",
                "Sns": {
                    "Type": "Notification",
                    "MessageId": "4cd796f7-9343-50ec-9a06-cab059bd0fcb",
                    "TopicArn": "arn:aws:sns:us-east-1:123901341784:NewGFSObject",
                    "Subject": "Amazon S3 Notification",
                    "Message": '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2023-01-22T23:37:33.401Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AROARZWIYOBMAI6ZTNURM:gfs-small-function-deb1cca"},"requestParameters":{"sourceIPAddress":"44.204.24.33"},"responseElements":{"x-amz-request-id":"1TQFHM7TQRP2746B","x-amz-id-2":"bTtBAOQb+tDvPMrcLIJOU1ufeM8BzJP3sbEtgxDRKXbTr28mObXw90coma5rWieAr/IUwbePxuidOeL0cWGKi+T0FY4MxpxQ"},"s3":{"s3SchemaVersion":"1.0","configurationId":"NjNmNjg3MWUtNTAzNy00YTcxLWI3ZGMtM2MzMjI2OGY5Y2Ey","bucket":{"name":"noaa-gfs-bdp-pds","ownerIdentity":{"principalId":"A2AJV00K47QOI1"},"arn":"arn:aws:s3:::noaa-gfs-bdp-pds"},"object":{"key":"gfs.20230122/18/atmos/gfs.t12z.pgrb2.0p25.f000","size":62122,"eTag":"80380c641b671c1bd8c92900406b030e","sequencer":"0063CDC8BD5633FC1D"}}}]}',
                    "Timestamp": "2023-01-22T23:37:34.519Z",
                    "SignatureVersion": "1",
                    "Signature": "mx1/jHp41Qk5t9VUJlR6MfMAnTzl5o0CforAtCeRX6kzf+7vFeB2WK5XNVmcfHD8nMzpyL9FbaFQjNnnc4Wx8bZFXN8MU9o2BE9hAMmxuu3W/eDIHMoomHEIu/8cdeKGJW3D0Vdj3MpbIXnv/6pKogIGk9F9xwidR6BMQoWWxwD1K2BPfGA+lzu+QmipSEAUqgx78hBYKwcfR7PMaCItIvdEHga2t94cBkpoZNf5EvjOoDXoYBGdBPfo0dXAkMIDZT+Xiz/zaJBgUoNF1zJpp+6uTklkud6yO0+M2T2C0i2lP37R6OyJozpteFyjIFiBXMjSv46y20FsqGVQQBQfDQ==",
                    "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-56e67fcb41f6fec09b0196692625d385.pem",
                    "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:123901341784:NewGFSObject:99e6fed7-5577-4bcc-a06d-3eb9101cbe66",
                    "MessageAttributes": {},
                },
            }
        ]
    }
    return dict(message)


@pytest.fixture
def non_gfs_grib_event():
    message = {
        "Records": [
            {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:us-east-1:123901341784:NewGFSObject:99e6fed7-5577-4bcc-a06d-3eb9101cbe66",
                "Sns": {
                    "Type": "Notification",
                    "MessageId": "4cd796f7-9343-50ec-9a06-cab059bd0fcb",
                    "TopicArn": "arn:aws:sns:us-east-1:123901341784:NewGFSObject",
                    "Subject": "Amazon S3 Notification",
                    "Message": '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2023-01-22T23:37:33.401Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AROARZWIYOBMAI6ZTNURM:gfs-small-function-deb1cca"},"requestParameters":{"sourceIPAddress":"44.204.24.33"},"responseElements":{"x-amz-request-id":"1TQFHM7TQRP2746B","x-amz-id-2":"bTtBAOQb+tDvPMrcLIJOU1ufeM8BzJP3sbEtgxDRKXbTr28mObXw90coma5rWieAr/IUwbePxuidOeL0cWGKi+T0FY4MxpxQ"},"s3":{"s3SchemaVersion":"1.0","configurationId":"NjNmNjg3MWUtNTAzNy00YTcxLWI3ZGMtM2MzMjI2OGY5Y2Ey","bucket":{"name":"noaa-gfs-bdp-pds","ownerIdentity":{"principalId":"A2AJV00K47QOI1"},"arn":"arn:aws:s3:::noaa-gfs-bdp-pds"},"object":{"key":"gfs.20230122/18/atmos/gfs.t12z.notGribstuff.0p25.f000","size":62122,"eTag":"80380c641b671c1bd8c92900406b030e","sequencer":"0063CDC8BD5633FC1D"}}}]}',
                    "Timestamp": "2023-01-22T23:37:34.519Z",
                    "SignatureVersion": "1",
                    "Signature": "mx1/jHp41Qk5t9VUJlR6MfMAnTzl5o0CforAtCeRX6kzf+7vFeB2WK5XNVmcfHD8nMzpyL9FbaFQjNnnc4Wx8bZFXN8MU9o2BE9hAMmxuu3W/eDIHMoomHEIu/8cdeKGJW3D0Vdj3MpbIXnv/6pKogIGk9F9xwidR6BMQoWWxwD1K2BPfGA+lzu+QmipSEAUqgx78hBYKwcfR7PMaCItIvdEHga2t94cBkpoZNf5EvjOoDXoYBGdBPfo0dXAkMIDZT+Xiz/zaJBgUoNF1zJpp+6uTklkud6yO0+M2T2C0i2lP37R6OyJozpteFyjIFiBXMjSv46y20FsqGVQQBQfDQ==",
                    "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-56e67fcb41f6fec09b0196692625d385.pem",
                    "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:123901341784:NewGFSObject:99e6fed7-5577-4bcc-a06d-3eb9101cbe66",
                    "MessageAttributes": {},
                },
            }
        ]
    }
    return dict(message)


def test_lambda_handler_enqueues_new_gfs_grib_events(new_gfs_grib_event):
    result = lambda_handler(new_gfs_grib_event, None)
    assert (
        result
        == "QUEUEING - bucket: noaa-gfs-bdp-pds key: gfs.20230122/18/atmos/gfs.t12z.pgrb2.0p25.f000"
    )


def test_lambda_handler_skips_non_gfs_grib_events(non_gfs_grib_event):
    result = lambda_handler(non_gfs_grib_event, None)
    assert (
        result
        == "SKIPPING - unknown filetype: gfs.20230122/18/atmos/gfs.t12z.notGribstuff.0p25.f000"
    )
