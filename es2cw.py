#!/usr/bin/python
from boto.ec2 import cloudwatch
import json
import yaml
import sys
## elasticsearch
from elasticsearch import Elasticsearch

for arg in sys.argv:
    env = arg

with open("config/" + env +".yml", 'r') as config_file:
    config = yaml.load(config_file)

ES_HOST = config['elasticsearch']['host']
ES_PORT = config['elasticsearch']['port']
aws = {}
aws["region"] = "us-west-1"

cw_metric_object = {}
cw_metric_object["namespace"] = "production_elasticsearch"
cw_metric_object["name"] = []
cw_metric_object["value"] = []
cw_metric_object["unit"] = []
cw_metric_object["dimensions"] = None
cw_metric_object["statistics"] = None
cw_metric_object["timestamp"] = None


def put_cloudwatch_metric():
  cw = cloudwatch.connect_to_region("us-west-1")
  cw.put_metric_data(cw_metric_object["namespace"],
  	                 cw_metric_object["name"],
  	                 value = cw_metric_object["value"],
  	                 unit = cw_metric_object["unit"],
  	                 timestamp = cw_metric_object["timestamp"],
  	                 dimensions = cw_metric_object["dimensions"],
  	                 statistics = cw_metric_object["statistics"])
  print "Successfully sent metrics to cloudwatch"



body = {
    "query": {
        "filtered": {
            "filter": {
                "range": {
                    "@timestamp": {
                        "gte": "now-2m",
                        "lte": "now"
                    }
                }
            }
        }
    },
    "size": 0,
    "aggs": {
        "minutes": {
            "date_histogram": {
                "field": "@timestamp",
                "interval": "day"
            },
            "aggs": {
                "nodes": {
                    "terms": {
                        "field": "node.name.raw",
                        "size": 10,
                        "order": {
                            "memory": "desc"
                        }
                    },
                    "aggs": {
                        "memory": {
                            "avg": {
                                "field": "jvm.mem.heap_used_percent"
                            }
                        }
                    }
                }
            }
        }
    }
}


def get_metric_data():
	es = Elasticsearch([{'host' : ES_HOST, 'port' : ES_PORT}])
	res = es.search(
		index = ".marvel-*",
		body = body)
	heap_stats = res["aggregations"]["minutes"]["buckets"][0]['nodes']['buckets']
	parsed = json.dumps(heap_stats, indent=4)
	for node in heap_stats:
		cw_metric_object["value"].append(node["memory"]["value"])
		cw_metric_object["name"].append("production_" + node["key"] + "_heap_memory")
		cw_metric_object["unit"].append("Percent")
		print "Added metrics for node " + node["key"]


get_metric_data()
put_cloudwatch_metric()
