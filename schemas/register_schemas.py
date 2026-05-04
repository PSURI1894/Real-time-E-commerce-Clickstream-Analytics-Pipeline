import requests
REGISTRY_URL = 'http://localhost:8081'
def register(subject, schema_dict):
    headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}
    payload = {'schema': json.dumps(schema_dict)}
    resp = requests.post(f'{REGISTRY_URL}/subjects/{subject}/versions', headers=headers, json=payload)
    return resp.json()
import json
if __name__ == '__main__':
    print('Schema Auto-registration execution script starting...')
