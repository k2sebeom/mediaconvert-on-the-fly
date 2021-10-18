import json
import boto3
from os import path

mediaconvert = boto3.client('mediaconvert', endpoint_url='https://qzovegirb.mediaconvert.ap-northeast-2.amazonaws.com')
s3 = boto3.client('s3')

JOB_TEMPLATE_NAME = "{MediaConvert Job Template}"
ROLE_ARN = "{MediaConvert Role Arn}"
SOURCE_BUCKET = "{Source S3 Bucket Name}"
DEST_BUCKET = "{Destination S3 Bucket Name}"

RESOLUTION_MAP = {
    "270": {
        "width": '480',
        "height": '270',
        "bitrate": "400000"
    },
    "360": {
        "width": '640',
        "height": '360',
        "bitrate": "600000"
    },
    "720": {
        "width": '1280',
        "height": '720',
        "bitrate": "5000000"
    },
    "1080": {
        "width": '1920',
        "height": '1080',
        "bitrate": "8500000"
    }
}


def create_master_template(obj_key):
    MASTER_MANIFEST = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT-SEGMENTS\n"
    template = mediaconvert.get_job_template(Name=JOB_TEMPLATE_NAME)
    outputs = template['JobTemplate']['Settings']['OutputGroups'][0]['Outputs']
    names = []
    for out in outputs:
        names.append(int(out['NameModifier'].replace('p', '')))
    names.sort()
    for name_mod in names:
        ABR_HEADER = '#EXT-X-STREAM-INF:BANDWIDTH={0},RESOLUTION={1}x{2}'
        bitrate = RESOLUTION_MAP[str(name_mod)]['bitrate']
        w, h = RESOLUTION_MAP[str(name_mod)]['width'], RESOLUTION_MAP[str(name_mod)]['height']
        MASTER_MANIFEST += ABR_HEADER.format(bitrate, w, h) + '\n' + '{0}' + f"{name_mod}.m3u8\n"
    obj_name = path.splitext(obj_key)[0]
    s3.put_object(Bucket=DEST_BUCKET, Key=f'{obj_name}/master_template.m3u8', Body=MASTER_MANIFEST.encode('utf8'))

def create_emc_job(obj_key):
    return mediaconvert.create_job(
        JobTemplate=JOB_TEMPLATE_NAME,
        Role=ROLE_ARN,
        Settings={
            'Inputs': [{
              'FileInput': f"s3://{SOURCE_BUCKET}/" + obj_key
            }],
            'OutputGroups': [{
                'OutputGroupSettings': {
                    'HlsGroupSettings': {
                        'Destination': f"s3://{DEST_BUCKET}/" + path.splitext(obj_key)[0] + "/"
                    }
                }
            }],
        })
        

def lambda_handler(event, context):
    rec = event['Records']
    resp = []
    for r in rec:
        create_master_template(r['s3']['object']['key'])
        resp.append(create_emc_job(r['s3']['object']['key']))

    return {
        'statusCode': 200,
        'body': f"Created {len(resp)} jobs"
    }
