from layers_gx import LayersGX
import layers_gx_conn
from datetime import datetime
s3_access_key = layers_gx_conn.AWS_KEY
s3_secret_key = layers_gx_conn.AWS_SECRET
aws_region = 'us-east-1'
import boto3
import os
from dotenv import load_dotenv
load_dotenv()

client = boto3.client(
    service_name='s3',
    aws_access_key_id=os.getenv('ds_access_key'),
    aws_secret_access_key=os.getenv('ds_secret_access_key'),
    region_name='us-east-1' 
)
response = client.list_objects_v2(Bucket='gaivota-data-science', Prefix='projetos/ourofino/results/')
keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith(".geojson")]
url_layers_gx = 'https://api-internal.gaivota.ai/api/layers-gx/v1'
for i in keys:
    filename = os.path.basename(i)
    print(filename)
    entity_name = filename.replace(" ", "_").split('.')[0]    
    print(entity_name)
    s3_path = 's3://gaivota-data-science/'+i
    print(s3_path)
    geom_type_str = 'Polygon'
    data_type_str = 'Private'
    include_gaivota_id = 'Y'
    org_id = 'Ourofino Staging'

    date_file = datetime.now().strftime('%Y-%m-%d')

    lgx = LayersGX()

    kwargs = {
        'aws_path_s3': s3_path,
        'aws_access_key': s3_access_key,
        'aws_security_key': s3_secret_key,
        'aws_region': aws_region,
        'low_zoom_detail': 12,
        'high_zoom_detail': 14,
    }

    data = lgx.insert_layers_gx(
        url_layers_gx,
        entity_name,
        org_id,
        geom_type_str,
        data_type_str,
        date_file,
        include_gaivota_id,
        **kwargs
    )
    print(data)


