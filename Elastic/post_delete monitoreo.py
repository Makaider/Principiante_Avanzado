POST /elastic*/_delete_by_query
{
  "query": {
    "match": {
      "log.level": "INFO"
    }
  }
}

DELETE /.monitoring*

PUT /*/_settings
{
  "index" : {
    "number_of_replicas" : 0
  }
}
