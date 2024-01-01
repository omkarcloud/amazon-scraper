from botasaurus.decorators_utils import create_directory_if_not_exists
from casefy import kebabcase


def kebab_case(s):
    return kebabcase(s)

def make_folders(query_kebab):
  create_directory_if_not_exists(f"output/{query_kebab}/")
  create_directory_if_not_exists(f"output/{query_kebab}/json/")
  create_directory_if_not_exists(f"output/{query_kebab}/csv/")

def format(query_kebab, type, name):
    return f"{name}-of-{query_kebab}.{type}"

