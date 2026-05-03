import json
def load_schema(name):
    with open(f'schemas/{name}.avsc') as f:
        return json.load(f)
