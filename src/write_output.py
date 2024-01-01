from botasaurus import bt
from botasaurus.decorators import print_filenames
from .write_output_utils import kebab_case, make_folders


def create_json(path, data):
    bt.write_json(data, path, False )


def create_csv(path, data):
    bt.write_csv(data, path, False )

def format(query_kebab, entity_type, type):
    return f"{query_kebab}.{type}"

def create(data, selected_fields, csv_path, json_path, query_kebab,entity_type):
        written = []
        
        fname = json_path + format(query_kebab, entity_type,"json",) 
        create_json(fname, data)
        written.append(fname)

        fname = csv_path + format(query_kebab, entity_type,"csv",) 
        create_csv(fname, data)
        written.append(fname)

        print_filenames(written)

def write_output(query, data, entity_type,transformer = kebab_case):

    query_kebab = transformer(query)
    make_folders(query_kebab)

    csv_path = f"output/{query_kebab}/csv/" 
    json_path = f"output/{query_kebab}/json/"

    create(data,[],  csv_path, json_path, query_kebab,entity_type)
