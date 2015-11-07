## postgres2elasticsearch

Handy little program to bulk insert large postgres databases into elasticsearch. It uses postgres's row_to_json function to
select the table columns as JSON and upload them using the bulk insert API. It was modeled after the grest jdbc bulk loader here: https://github.com/jprante/elasticsearch-jdbc

However, it waits for a response from ElasticSearch which prevents falied inserts when the ElasticSearch bulk loader queue is full.

#### Requirements

- Postgres 9.2 or greater (for JSON functions)

#### Usage

```
$ postgres2elasticsearch <config.json> [number_of_workers]
```

Sample config file:

```
{
  "url" : "http://localhost:9200",
  "index" : "cats",
  "type" : "house",
  "max_bulk_actions" : 1000,
  "db" : {
    "host" : "localhost",
    "port" : 5432,
    "user" : "postgres",
    "password" : "thisisapassword",
    "database" : "postgres",
    "table" : "cats"
  },
  "mappings" : [{
    "house" : {
      "properties" : {
        "name" : {
          "type" : "string",
          "index" : "not_analyzed"
        }
      }
    }
  }]
}
```

#### Troubleshooting

If you start to get failed insert requests, try decreasing the number of workers. Also, **max_bulk_actions** correlates to the number of inserts in a bulk request. So increasing that number might help some. However, to big of a number and the elasticsearch thread might take too long and timeout.  
