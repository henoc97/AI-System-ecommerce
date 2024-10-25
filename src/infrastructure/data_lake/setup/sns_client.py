import boto3

# Create a SNS client
sns = boto3.client('sns', endpoint_url="http://localhost:4566")

# Create a SNS topic
response = sns.create_topic(Name='MyTopic')

# Subscribe to the SNS topic
sns.subscribe(
    TopicArn=response['TopicArn'],
    Protocol='email',
    Endpoint='nononephew@gmail.com'
)