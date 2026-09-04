# DynamoDB

## The Problem

You need a fully managed, infinitely scalable key-value/document store with single-digit millisecond latency, zero operational overhead.

---

## The Data Model

DynamoDB tables have:
- **Partition key** (required): determines data placement
- **Sort key** (optional): orders items within a partition, enables range queries

```python
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserEvents')

# Write
table.put_item(Item={
    'user_id': 'u001',        # partition key
    'timestamp': '2024-01-15T10:00:00Z',  # sort key
    'event_type': 'login',
    'ip': '203.0.113.1'
})

# Read all events for a user (efficient: single partition)
response = table.query(
    KeyConditionExpression=Key('user_id').eq('u001')
)

# Read events in a time range (efficient: sort key range)
response = table.query(
    KeyConditionExpression=Key('user_id').eq('u001') &
                           Key('timestamp').between('2024-01-15', '2024-01-16')
)
```

---

## Single-Table Design

DynamoDB's best practice for relational-like access patterns: store multiple entity types in one table using overloaded partition/sort keys.

```
PK           | SK              | ...
-------------|-----------------|----
USER#u001    | PROFILE         | name, email
USER#u001    | ORDER#o001      | amount, status
USER#u001    | ORDER#o002      | amount, status
ORDER#o001   | ITEM#i001       | product, qty
```

This allows efficient "get user and all their orders" in a single query:

```python
table.query(
    KeyConditionExpression=Key('PK').eq('USER#u001') &
                           Key('SK').begins_with('ORDER#')
)
```

---

## When to Use DynamoDB

- Serverless applications needing zero-ops storage
- Simple key-value access patterns at any scale
- AWS-native applications
- Known access patterns that don't require ad-hoc queries

## When NOT to Use DynamoDB

- Complex queries, aggregations, analytics
- Flexible schema changes at query time
- Cost sensitivity at high read volume (DynamoDB pricing can be expensive)
- Multi-table JOINs
