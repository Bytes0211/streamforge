#!/usr/bin/env python3
import json
import re
import sys

def fix_mongodb_json(input_file, output_file):
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Apply MongoDB extended JSON transformations
    # 1. Handle ObjectId, Long, ISODate constructors
    content = re.sub(r"ObjectId\('([^']+)'\)", r'"\1"', content)
    content = re.sub(r"Long\('([^']+)'\)", r'\1', content)
    content = re.sub(r"ISODate\('([^']+)'\)", r'"\1"', content)
    
    # 2. Add quotes to keys
    content = re.sub(r'(\s+)(_id|data|timestamp|processedAt):', r'\1"\2":', content)
    
    # 3. Convert single-quoted string values to double-quoted
    # Match: key: 'value' and replace with key: "value"
    # Need to escape double quotes inside the value
    content = re.sub(r":\s*'([^']*)'", lambda m: ': "' + m.group(1).replace('"', '\\"') + '"', content)
    
    # Parse documents by finding { } pairs
    fixed_docs = []
    depth = 0
    current_doc = []
    
    for char in content:
        if char == '{':
            if depth == 0:
                current_doc = []
            current_doc.append(char)
            depth += 1
        elif char == '}':
            depth -= 1
            current_doc.append(char)
            if depth == 0 and current_doc:
                doc_str = ''.join(current_doc)
                try:
                    parsed = json.loads(doc_str)
                    fixed_docs.append(json.dumps(parsed))
                except json.JSONDecodeError as e:
                    print(f'Warning: Skipping invalid document: {e}')
                current_doc = []
        elif depth > 0:
            current_doc.append(char)
    
    # Write each document on its own line (JSONL format)
    with open(output_file, 'w') as f:
        for doc in fixed_docs:
            f.write(doc + '\n')
    
    print(f'✓ Fixed {len(fixed_docs)} documents')
    
    if fixed_docs:
        # Validate the first document
        first_doc = json.loads(fixed_docs[0])
        print(f'✓ Valid JSON - Sample document:')
        print(json.dumps(first_doc, indent=2))
    else:
        print('✗ No valid documents found')
        sys.exit(1)

if __name__ == '__main__':
    input_file = '/home/scotton/dev/projects/streamforge/docker/mongodb_export_processed_data.json'
    output_file = '/home/scotton/dev/projects/streamforge/docker/mongodb_export_processed_data_fixed.json'
    fix_mongodb_json(input_file, output_file)
    print(f'\nFixed file saved to: {output_file}')
