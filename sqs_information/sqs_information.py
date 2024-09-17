import boto3
import pandas as pd
from datetime import datetime, timedelta


# Adjust timezone
def adjust_timezone(timestamp):
    gmt_minus_3 = timestamp - timedelta(hours=3)
    return gmt_minus_3.strftime("%H:%M")


# Get SQS Queue Metrics
def get_queue_metrics(sqs_queue_url, cloudwatch_client, metric_start_time, metric_end_time):
    try:
        cloudwatch_sqs_namespace = 'AWS/SQS'

        sent_metrics = cloudwatch_client.get_metric_statistics(
            Namespace=cloudwatch_sqs_namespace,
            MetricName='NumberOfMessagesSent',
            Dimensions=[{'Name': 'QueueName', 'Value': sqs_queue_url.split('/')[-1]}],
            StartTime=metric_start_time,
            EndTime=metric_end_time,
            Period=3600,
            Statistics=['Sum']
        )

        received_metrics = cloudwatch_client.get_metric_statistics(
            Namespace=cloudwatch_sqs_namespace,
            MetricName='NumberOfMessagesReceived',
            Dimensions=[{'Name': 'QueueName', 'Value': sqs_queue_url.split('/')[-1]}],
            StartTime=metric_start_time,
            EndTime=metric_end_time,
            Period=3600,
            Statistics=['Sum']
        )

        latency_metrics = cloudwatch_client.get_metric_statistics(
            Namespace=cloudwatch_sqs_namespace,
            MetricName='ApproximateAgeOfOldestMessage',
            Dimensions=[{'Name': 'QueueName', 'Value': sqs_queue_url.split('/')[-1]}],
            StartTime=metric_start_time,
            EndTime=metric_end_time,
            Period=3600,
            Statistics=['Average']
        )

        # Processar dados
        sent_messages = [(dp['Sum'], dp['Timestamp']) for dp in sent_metrics['Datapoints']] if sent_metrics[
            'Datapoints'] else []
        received_messages = [(dp['Sum'], dp['Timestamp']) for dp in received_metrics['Datapoints']] if received_metrics[
            'Datapoints'] else []
        latencies = [(dp['Average'], dp['Timestamp']) for dp in latency_metrics['Datapoints']] if latency_metrics[
            'Datapoints'] else []

        avg_sent_result = sum([msg[0] for msg in sent_messages]) / len(sent_messages) if sent_messages else 0
        avg_received_result = sum([msg[0] for msg in received_messages]) / len(
            received_messages) if received_messages else 0
        avg_latency_result = sum([lat[0] for lat in latencies]) / len(latencies) if latencies else None

        peak_sent_result = max(sent_messages, key=lambda x: x[0])[0] if sent_messages else 0
        peak_sent_time_result = adjust_timezone(max(sent_messages, key=lambda x: x[0])[1]) if sent_messages else "N/A"

        peak_received_result = max(received_messages, key=lambda x: x[0])[0] if received_messages else 0
        peak_received_time_result = adjust_timezone(
            max(received_messages, key=lambda x: x[0])[1]) if received_messages else "N/A"

        peak_latency_result = max(latencies, key=lambda x: x[0])[0] if latencies else 0
        peak_latency_time_result = adjust_timezone(max(latencies, key=lambda x: x[0])[1]) if latencies else "N/A"

        return (avg_sent_result, avg_received_result, avg_latency_result, peak_sent_result, peak_sent_time_result,
                peak_received_result, peak_received_time_result, peak_latency_result, peak_latency_time_result)

    except Exception as e:
        print(f"Error getting metrics for queue {sqs_queue_url}: {str(e)}")
        return [None] * 9


# Initialize SQS Client
sqs = boto3.client('sqs')
cloudwatch = boto3.client('cloudwatch')

# Get all queues
queues = sqs.list_queues()['QueueUrls']

# Define time range
start_time = datetime(2024, 8, 1)
end_time = datetime(2024, 8, 31, 23, 59, 59)

# Result list
data = []

for queue_url in queues:
    print(f"Checking queue metrics: {queue_url}")

    (avg_sent, avg_received, avg_latency, peak_sent, peak_sent_time, peak_received, peak_received_time, peak_latency,
     peak_latency_time) = get_queue_metrics(
        queue_url, cloudwatch, start_time, end_time
    )

    # Collect queue data
    data.append([
        queue_url.split('/')[-1],
        avg_sent,
        avg_received,
        avg_latency,
        peak_sent,
        peak_sent_time,
        peak_received,
        peak_received_time,
        peak_latency,
        peak_latency_time
    ])

# Define report column names
columns = [
    'QueueName',
    'AverageMessagesSentPerDay (Count)',
    'AverageMessagesReceivedPerDay (Count)',
    'AverageProcessingLatency (Seconds)',
    'PeakMessagesSent (Count)',
    'PeakSentHour',
    'PeakMessagesReceived (Count)',
    'PeakReceivedHour',
    'PeakProcessingLatency (Seconds)',
    'PeakLatencyHour'
]

df = pd.DataFrame(data, columns=columns)

df.to_csv('sqs_metrics.csv', index=False)

print("Report generated: sqs_metrics.csv")
