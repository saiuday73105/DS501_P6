##################################################################################
############################# DS501 Assignment - 1 ###############################
################################## Part - 6 ######################################
#### Group 17 - Acquire - GVN Sai Uday, Y. Saivenkat, Yennam Krishna Vamshi ######
##################################################################################
import json
from flask import Flask, render_template, request, jsonify
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(['https://elastic:MX8Hirhe64sk8A7u62QeTn7b@1fb485351087468e860190f7b500eed8.eastus2.azure.elastic-cloud.com:9243'])
app = Flask(__name__)

with open('./netflix.json', encoding='utf-8') as file:
    data = json.load(file)

def addDetails(key):
    dict_data = data[key]
    data[key] = {'_id' : key, '_index' : 'netflix'}
    data[key].update(dict_data)

for item in data:
    addDetails(item)

data_list = []
for i in data.keys():
    data_list.append(data[i])

grpNo = 17
N = (grpNo % 4) + 2
mapping = {
  "settings": {
    "netflix": {
      "max_ngram_diff": 0
    },
    "analysis": {
      "analyzer": {
        "default": {
          "tokenizer": "whitespace",
          "filter": [ "Ngram_filter" ]
        }
      },
      "filter": {
        "3_gram_filter": {
          "type": "ngram",
          "min_gram": N,
          "max_gram": N
        }
      }
    }
  }
}

if(not(es.indices.exists(index='netflix'))):
    es.indices.create(index="netflix",body=mapping,ignore=400)
    helpers.bulk(es, data_list)


@app.route('/autoCompAdults', methods=['GET'])
def autoCompAdults():
    keyword = request.args.get('query')
    body = {"size": 5,"query": {"bool": {"must": [{"match_phrase_prefix": {"title": {"query": keyword}}}]}}}
    response = es.search(index="netflix", body=body)

    return jsonify(response['hits']['hits'])

@app.route('/autoCompChildProof', methods=['GET'])
def autoCompChildProof():
    keyword = request.args.get('query')

    body = {"size": 5,"query": {"bool": {"must": [{"match_phrase_prefix": {"title": {"query": keyword}}}],
                          "must_not": [
                          {"match": {"rating": "R"}},
                          {"match": {"rating": "NC"}},
                          {"match": {"rating": "PG"}}]}}}

    response = es.search(index="netflix", body=body)

    return jsonify(response['hits']['hits'])

@app.route('/paginationMovies', methods=['GET'])
def paginationMovie():
    pageNum = int(request.args.get('pageNum'))
    pageSize = int(request.args.get('pageSize'))
    start = pageSize * pageNum - (pageSize - 1)

    body = {"from": start,"size": pageSize,"query": {"bool": {
                    "must": [{"match": {"type": "Movie"}}]}},
            "sort" : [{"release_year" : {"order" : "desc"}}]}

    response = es.search(index="netflix", body=body)

    return jsonify(response['hits']['hits'])

@app.route('/paginationTVShows', methods=['GET'])
def paginationTVShows():
    pageNum = int(request.args.get('pageNum'))
    pageSize = int(request.args.get('pageSize'))
    start = pageSize * (pageNum - 1) + 1

    body = {"from": start,"size": pageSize,"query": {"bool": {"must": [
                        {"match": {"type": "TV Shows"}}]}},
            "sort" : [{"release_year" : {"order" : "desc"}}]}

    response = es.search(index="netflix", body=body)

    return jsonify(response['hits']['hits'])

@app.route('/exactMatch', methods=['GET'])
def exactMatch():
    specifiedField = request.args.get('field')
    keyword = request.args.get('query')
    body = {"query": {"match": {specifiedField: keyword}}}
    response = es.search(index="netflix", body=body)

    return jsonify(response['hits']['hits'])

@app.route('/prefixMatch', methods=['GET'])
def prefixMatch():
    keyword = request.args.get('query')
    body = {"query": {"span_first": {"match": {"span_term": {"description": keyword}},"end": 1}}}

    response = es.search(index="netflix", body=body)

    return jsonify(response['hits']['hits'])

@app.route('/genreMatch', methods=['GET'])
def genreMatch():
    keyword = request.args.get('query')
    keyword.upper()###
    body = {"query": {"query_string": {"query": keyword,"default_field": "listed_in"}}}

    response = es.search(index="netflix", body=body)

    return jsonify(response['hits']['hits'])

if __name__ == '__main__':
  app.run(debug=True)