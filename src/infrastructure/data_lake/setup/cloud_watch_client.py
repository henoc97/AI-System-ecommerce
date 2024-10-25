import boto3

# Create a CloudWatch client
cloudwatch = boto3.client('cloudwatch', endpoint_url="http://localhost:4566")

# Create a CloudWatch alarm
cloudwatch.put_metric_alarm(
    AlarmName='S3ErrorAlarm',
    MetricName='4xxErrors',
    Namespace='AWS/S3',
    Statistic='Sum',
    Period=300,
    EvaluationPeriods=1,
    Threshold=1,
    ComparisonOperator='GreaterThanOrEqualToThreshold',
    AlarmActions=[
        'arn:aws:sns:us-east-1:123456789012:MyTopic'
    ],
    Dimensions=[
        {
            'Name': 'BucketName',
            'Value': 'ecommerce-datalake'
        },
    ]
)