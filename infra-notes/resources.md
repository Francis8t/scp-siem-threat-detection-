# Live AWS resources (us-east-1)

| Resource | Name | Notes |
|---|---|---|
| Kinesis stream    | scp-siem-stream       | 1 shard, provisioned. PK=source_ip. |
| DynamoDB table    | scp-siem-speed-state  | PK=source_ip, SK=bucket. On-demand. TTL on `ttl`. |

Teardown at end of project (console: delete via each service, or CLI):
  aws kinesis delete-stream --stream-name scp-siem-stream
  aws dynamodb delete-table --table-name scp-siem-speed-state


  s3-bucket-name: scp-siem-data-009910375264

  the Firehose stream: scp-siem-firehose 

  Firehose IAM role name: KinesisFirehoseServiceRole-scp-siem-fire-us-east-1-1784818029868
