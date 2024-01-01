from src import Amazon

queries = [
  "Mango",
  "Watermelon",
]

Amazon.search(queries, max=10)
