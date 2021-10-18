import json
import boto3

s3 = boto3.client('s3')
emc = boto3.client('mediaconvert', endpoint_url='https://qzovegirb.mediaconvert.ap-northeast-2.amazonaws.com')

def lambda_handler(event, context):
    records = event['Records']
    resp = []
    
    for rec in records:
        obj_key = rec['s3']['object']['key']
        bucket_name = rec['s3']['bucket']['name']
        base_name = obj_key.split('/')[0]
        
        payload = { "file": base_name }
        
        manifest = s3.get_object(Bucket=bucket_name, Key=obj_key)['Body'].read()
        if b'ENDLIST' in manifest:
            s3.put_object(Bucket=bucket_name, Key=obj_key.replace('p.m3u8', '.m3u8'), Body=manifest)
            payload["message"] = "Transcoding is Finished"
            payload["body"] = manifest.decode()
        else:
            lines = manifest.decode().split('\n')
            for i, line in enumerate(lines):
                if 'MEDIA-SEQUENCE' in line:
                    lines.insert(i + 1, '#EXT-X-START:TIME-OFFSET=0')
                    break
            manifest = '\n'.join(lines).encode()
            new_manifest = manifest.replace(b'VOD', b'EVENT')
            
            s3.put_object(Bucket=bucket_name, Key=obj_key.replace('p.m3u8', '.m3u8'), Body=new_manifest)
            payload["message"] = "Transcoding is in progress"
            payload["body"] = new_manifest.decode()
        
        # Checks for master.m3u8
        try:
            s3.get_object(Bucket=bucket_name, Key=f'{base_name}/master.m3u8')
        except Exception:
            template = s3.get_object(Bucket=bucket_name, Key=f'{base_name}/master_template.m3u8')['Body'].read().decode()
            s3.put_object(Bucket=bucket_name, Key=f'{base_name}/master.m3u8', Body=template.format(base_name).encode('utf8'))
            payload["master"] = template.format(base_name)
        
        resp.append(payload)

    return {
        'statusCode': 200,
        'body': resp
    }
