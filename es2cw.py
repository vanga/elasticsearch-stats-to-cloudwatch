#!/usr/bin/python
from boto.ec2 import cloudwatch
import json
import yaml
import sys
import time
## elasticsearch
from elasticsearch import Elasticsearch

for arg in sys.argv:
    env = arg

with open("config/" + env +".yml", 'r') as config_file:
    config = yaml.load(config_file)

ES_HOST = config['elasticsearch']['host']
ES_PORT = config['elasticsearch']['port']
aws = {}
aws_region = config['aws']['region']
aws["region"] = aws_region
metrics_to_monitor = ["heap_percent","search_queue","gc_young","gc_old","fd_evicts","qc_evicts"]

def put_cloudwatch_metric(cw_metric_object):
  print cw_metric_object,"cw_metric_object"
  cw = cloudwatch.connect_to_region(aws_region)
  cw.put_metric_data(cw_metric_object["namespace"],
                     cw_metric_object["name"],
                     value = cw_metric_object["value"],
                     unit = cw_metric_object["unit"],
                     timestamp = cw_metric_object["timestamp"],
                     dimensions = cw_metric_object["dimensions"],
                     statistics = cw_metric_object["statistics"])
  print "Successfully sent metrics to cloudwatch"


metrics = {
    "heap_percent": {
        "type": "avg",
        "path":"node_stats.jvm.mem.heap_used_percent",
        "unit": "Percent"
    },
    "search_queue": {
        "type": "avg",
        "path": "node_stats.thread_pool.search.queue",
        "unit": "Count"
    },
    "gc_young": {
        "type": "derivative",
        "path": "node_stats.jvm.gc.collectors.young.collection_time_in_millis",
        "unit": "Count"  
    },
    "gc_old": {
        "type": "derivative",
        "path": "node_stats.jvm.gc.collectors.old.collection_time_in_millis",
        "unit": "Count"
    },
    "fd_evicts": {
        "type": "derivative",
        "path": "node_stats.indices.fielddata.evictions",
        "unit": "Count"
    },
    "qc_evicts": {
        "type": "derivative",
        "path": "node_stats.indices.query_cache.evictions",
        "unit": "Count"
    }
}

def get_es_query(metric):
    if metrics[metric]["type"] == "avg":
        es_query = {"query":{"bool":{"must":{"range":{"timestamp":{"gte":"now-2m","lte":"now"}}}}},"size":0,"aggs":{"nodes":{"terms":{"field":"source_node.name","size":1},"aggs":{"metrics":{"date_histogram":{"field":"timestamp","interval":"60s"},"aggs":{metric:{"avg":{"field":metrics[metric]["path"]}}}}}}}}
    elif metrics[metric]["type"] == "derivative":
        cur_time = int(time.time() * 1000)
        prev_time = cur_time - 600000
        es_query = {"size":0,"query":{"bool":{"filter":[{"range":{"timestamp":{"format":"epoch_millis","gte":prev_time,"lte":cur_time}}}]}},"aggs":{"nodes":{"terms":{"field":"source_node.name"},"aggs":{"metrics":{"date_histogram":{"field":"timestamp","min_doc_count":0,"interval":"60s","extended_bounds":{"min":prev_time,"max":cur_time}},"aggs":{metric:{"max":{"field":metrics[metric]["path"]}},metric+"_deriv":{"derivative":{"buckets_path":metric,"gap_policy":"skip"}}}}}}}}
    else:
        raise Exception("Metric is unknown to generate ES query")
    return es_query

def get_base_cw_metric_object():
    cw_metric_object = {}
    cw_metric_object["namespace"] = env+"_elasticsearch"
    cw_metric_object["name"] = []
    cw_metric_object["value"] = []
    cw_metric_object["unit"] = []
    cw_metric_object["dimensions"] = None
    cw_metric_object["statistics"] = None
    cw_metric_object["timestamp"] = None 
    return cw_metric_object    

def get_metric_data(metric, cw_metric_object):
    metric_unit = metrics[metric]['unit']
    if metrics[metric]["type"] == "avg":
        value_key = metric
    elif metrics[metric]["type"] == "derivative":
        value_key = metric+"_deriv"
    es = Elasticsearch([{'host' : ES_HOST, 'port' : ES_PORT}])
    es_query = get_es_query(metric)
    res = es.search(
        index = ".monitoring-es-*",
        body = es_query)
    es_stats = res["aggregations"]["nodes"]["buckets"]

    parsed = json.dumps(es_stats, indent=4)
    for node_stats in es_stats:
        print node_stats["metrics"]["buckets"][-1]
        node_name = node_stats["key"]
        if value_key not in node_stats["metrics"]["buckets"][-1]:
            continue
        metric_value = node_stats["metrics"]["buckets"][-1][value_key]["value"]
        cw_metric_object["value"].append(metric_value)
        cw_metric_object["name"].append(env+"_" + node_name + "_"+metric)
        cw_metric_object["unit"].append(metric_unit)
        print "Added metrics for node " + node_name,metric+":",metric_value
    return cw_metric_object
    # search queue monitoring
    # res = es.search(
    #     index = ".marvel-*",
    #     body = search_queue_query)
    # sq_stats = res["aggregations"]["minutes"]["buckets"][0]['nodes']['buckets']
    # parsed = json.dumps(sq_stats, indent=4)
    # for node in sq_stats:
    #     cw_metric_object["value"].append(node["searchqueue"]["value"])
    #     cw_metric_object["name"].append("production_" + node["key"] + "_search_queue")
    #     cw_metric_object["unit"].append("Count")
    #     print "Added metrics for node " + node["key"]


cw_metric_object = get_base_cw_metric_object()
for metric in metrics_to_monitor:
    cw_metric_object = get_metric_data(metric,cw_metric_object)
    put_cloudwatch_metric(cw_metric_object)
