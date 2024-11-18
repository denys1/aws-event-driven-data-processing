import json
import boto3
import os

def process(event, context):
    try:
        s3_client = boto3.client('s3')
        # input_bucket = os.environ['INPUT_BUCKET']
        output_bucket = os.environ['OUTPUT_BUCKET']

        for record in event['Records']:
            # Parse SQS message
            body = json.loads(record['body'])
            
            # Get S3 object details
            s3_event = json.loads(body['Message'])
            bucket = s3_event['detail']['requestParameters']['bucketName']
            key = s3_event['detail']['requestParameters']['key']

            # Download JSON file
            response = s3_client.get_object(Bucket=bucket, Key=key)
            json_data = json.loads(response['Body'].read().decode('utf-8'))

            # Process the JSON data (add your processing logic here)
            # For now, we'll just pass through the data
            processed_data = json_data

            # Upload processed JSON back to S3
            output_key = key.replace('.json', '_processed.json')
            s3_client.put_object(
                Bucket=output_bucket,
                Key=output_key,
                Body=json.dumps(processed_data),
                ContentType='application/json'
            )

        return {
            'statusCode': 200,
            'body': json.dumps('Processing completed successfully')
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        raise e
