#!/usr/bin/env python3
import json
import sys
from datetime import datetime
def transform_event(mongo_doc):
    """Transform MongoDB event document to DynamoDB format"""
    return {
        'id': {'S': mongo_doc.get('id', str(mongo_doc['_id']))},
        'timestamp': {'N': str(mongo_doc.get('timestamp', 0))},
        'type': {'S': mongo_doc.get('type', 'unknown')},
        'userId': {'S': mongo_doc.get('userId', 'unknown')},
        'value': {'N': str(mongo_doc.get('value', 0))},
        'payload': {'S': mongo_doc.get('payload', '')},
        'processedAt': {'N': str(int(datetime.now().timestamp() * 1000))}
    }
def main():
    # Use paths as provided
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    with open(input_file, 'r') as f:
        content = f.read()
        mongo_docs = json.loads(content)
    
    dynamodb_items = [transform_event(doc) for doc in mongo_docs]
    
    # Create batch write requests
    batch_requests = []
    for i in range(0, len(dynamodb_items), 25):
        batch = dynamodb_items[i:i+25]
        batch_requests.append({
            'streamforge-processed-data-dev': [
                {'PutRequest': {'Item': item}} for item in batch
            ]
        })
    
    with open(output_file, 'w') as f:
        json.dump(batch_requests, f, indent=2)
if __name__ == '__main__':
    main()
