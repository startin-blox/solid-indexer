import requests
import json
import os
from urllib.parse import urlparse
from apscheduler.schedulers.blocking import BlockingScheduler

# List of base URLs of the servers
servers = [
    'http://localhost:8000/',
    'http://localhost:8001/'
    # Add more servers as needed
]

def fetch_data(url):
    """Fetch data from a given URL."""
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()

def save_data(url, data, save_as_file=False):
    """Save JSON data to a local file, maintaining the directory structure."""
    parsed_url = urlparse(url)
    path = parsed_url.path.lstrip('/')

    if save_as_file:
        path = path.rstrip('/')
        # Save data directly to a file, treat path as file name
        file_path = path + '.jsonld'

        # Read existing data if it exists
        old_data = None
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                old_data = json.load(f)

        if old_data:
            # Compute differences
            differences = diff_references(old_data, data)
            if differences:
                print(f"Differences found in {url}: {differences}")

                # Merge old and new data without duplicates
                old_graph = {item['@id']: item for item in old_data.get('@graph', [])}
                new_graph = {item['@id']: item for item in data.get('@graph', [])}
                merged_graph = {**old_graph, **new_graph}  # merge dictionaries, new_data will overwrite old_data for duplicate keys

                merged_data = {
                    "@context": old_data.get("@context"),
                    "@graph": list(merged_graph.values())
                }
            else:
                merged_data = data
        else:
            merged_data = data
        
        with open(file_path, 'w') as f:
            json.dump(merged_data, f, indent=2)
    else:
        if not path:
          path = './'
        dir_name = os.path.dirname(path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        with open(path + '.jsonld', 'w') as f:
            json.dump(data, f, indent=2)

def read_local_data(path):
    """Read JSON data from a local file."""
    if os.path.exists(path + '.jsonld'):
        with open(path + '.jsonld', 'r') as f:
            return json.load(f)
    return None

def diff_references(old_data, new_data):
    """Compare references between old and new data."""
    old_references = set(item['@id'] for item in old_data.get('@graph', []))
    new_references = set(item['@id'] for item in new_data.get('@graph', []))
    return new_references - old_references
  
def get_public_type_index(root_data):
    """Extract solid:publicTypeIndex from root data."""
    for item in root_data.get('@graph', []):
        if 'solid:publicTypeIndex' in item:
            return item['solid:publicTypeIndex']
    return None

def get_instance_containers(public_type_index_data):
    """Extract solid:instanceContainer from publicTypeIndex data."""
    instance_containers = []
    for item in public_type_index_data.get('@graph', []):
        if item.get('@type') == 'solid:TypeIndexRegistration' and item.get('solid:forClass') == 'ex:Index':
            instance_containers.append(item['solid:instanceContainer'])
    return instance_containers

def fetch_indexes(instance_container_url):
    """Fetch indexes from an instance container URL."""
    indexes_data = fetch_data(instance_container_url)
    save_data(instance_container_url, indexes_data)
    indexes = [item['@id'] for item in indexes_data.get('@graph', []) if item.get('@type') == 'ex:Index']
    return indexes

def process_indexes(url, aggregated_data, save_as_file=False):
    """Process indexes recursively."""
    data = fetch_data(url)
    save_data(url, data, save_as_file)
    print(f"Processing indexes for {url}")
    if url not in aggregated_data['indexes']:
        aggregated_data['indexes'][url] = data

    for item in data.get('@graph', []):
        if item.get('@id') == url:
            continue
        if item.get('@type') == 'ex:PropertyIndexRegistration':
            instances_in = item.get('ex:instancesIn') or item.get('rdfs:seeAlso')
            if instances_in:
                process_indexes(instances_in, aggregated_data, True)
        elif item.get('@type') == 'ex:Index':
            process_indexes(item['@id'], aggregated_data)

def aggregate_data():
    """Aggregate data from all servers and endpoints."""
    aggregated_data = {
        'indexes': {},
        'users': []
    }
    
    for server in servers:
        try:
            # Step 1: Fetch root data
            root_data = fetch_data(server)
            save_data(server, root_data)
            
            # Step 2: Extract solid:publicTypeIndex
            public_type_index_url = get_public_type_index(root_data)
            if not public_type_index_url:
                print(f"No publicTypeIndex found for {server}")
                continue

            # Step 3: Fetch publicTypeIndex data
            public_type_index_data = fetch_data(public_type_index_url)

            # Step 4: Extract instance containers
            instance_containers = get_instance_containers(public_type_index_data)
            print(f"Processing instance containers for {server}: {instance_containers}")

            for container_url in instance_containers:
                # Step 5: Fetch and process indexes
                process_indexes(container_url, aggregated_data)

        except requests.RequestException as e:
            print(f"Error fetching data from {server}: {e}")
            pass

    return aggregated_data

def run_crawler():
    """Run the crawler and output the aggregated JSON."""
    aggregated_data = aggregate_data()
    aggregated_json = json.dumps(aggregated_data, indent=2)
    
    # Print the aggregated JSON
    # print(aggregated_json)
    
    # Optionally, save to a file
    with open('aggregated_data.json', 'w') as f:
        f.write(aggregated_json)

if __name__ == "__main__":
    # Set up the scheduler
    scheduler = BlockingScheduler()
    
    # Schedule the crawler to run every X hours (e.g., every 6 hours)
    # X = 6  # Change X to the number of hours you need
    # scheduler.add_job(run_crawler, 'interval', hours=X)
    
    run_crawler()
    
    try:
        print(f"Scheduler started. The crawler will run every {X} hours.")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
