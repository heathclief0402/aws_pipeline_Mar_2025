import json
import boto3
import urllib.parse

def lambda_handler(event, context): # the lambda fucntion called: trigger-glue-lewis
    glue = boto3.client('glue')
    
    # Get bucket and file info from S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
    
    file_name = object_key.split('/')[-1]

    response = glue.start_job_run(
        JobName='consolidate-employee-data-lewis',
        Arguments={
            '--dst_bucket_name': bucket_name,
            '--file_name': file_name
            # Add more if needed
        }
    )
    print(
        {
    'statusCode': 200,
    'body': json.dumps({
        'message': 'Glue job triggered successfully',
        'bucket': bucket_name,
        'file': file_name,
        'jobRunId': response["JobRunId"]
    })
}
    )

    return {
    'statusCode': 200,
    'body': json.dumps({
        'message': 'Glue job triggered successfully',
        'bucket': bucket_name,
        'file': file_name,
        'jobRunId': response["JobRunId"]
    })
}