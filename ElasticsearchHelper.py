"""
File: ElasticsearchHelper
Author: Ian Ross
Email: iross@cs.wisc.edu
Description:
    A Helper class to provide an interface to the Elasticsearch component of the
    GeoDeepDive infrastructure. Allows easy creation of the standard indexes (with
    custom analyzers), creation/usage of percolators, scan-and-scroll search
    implementation, etc.
"""

import os, sys
import elasticsearch
import pymongo
from bson.objectid import ObjectId
import time
import GddConfig, urllib
import requests
import json
from datetime import datetime
import re

try:
    from settings import *
except ImportError:
    print "Could not import settings!"
    sys.exit(1)

LIMIT = False

class ElasticsearchHelper(object):
    """
    Helper class for common GDD Elasticsearch interactions

    Args:
        es_index (string) : The name of the Elasticsearch index to interact with.
    """
    def __init__(self, es_index=None, es_type=None):
        config = GddConfig.GddConfig()
        self.config = config

        self.es_url = config.get('es_url')
        self.es_user = config.get('es_user')
        self.es_password = config.get('es_password')
        self.index = config.get('es_index') if es_index is None else es_index
        self.es_type = config.get('es_type') if es_type is None else es_type

        if self.es_user is not None and self.es_password is not None:
            self.es_url = self.es_url.replace("http://", "http://%s:%s@" % (self.es_user, self.es_password))

        self.es_index_url = "%s/%s/" % (self.es_url, self.index)
        self.es_type_url =  "%s/%s" % (self.es_index_url, self.es_type)
#        self.es_url = "%s/%s/%s" % (self.es_url, self.index, self.es_type)
        self.es = elasticsearch.Elasticsearch([self.es_url], connection_class=elasticsearch.RequestsHttpConnection, timeout=30)
        self.create_index(self.index)

    def update_mapping(self, index_name):
        """
        """
        mapping = {
                self.es_type: {
                    "_source" : { "enabled" : True },
                    "properties" : {
                        "URL" : { "type" : "string", "index" : "not_analyzed" },
                        "authors" : { "type" : "string" },
                        "endingPage" : { "type" : "string", "index" : "not_analyzed" },
                        "filepath" : { "type" : "string", "index" : "not_analyzed" },
                        "issue" : { "type" : "string", "index" : "not_analyzed" },
                        "pubname" : { "type" : "string", "fields": {"full":{"type": "string", "index": "not_analyzed" } } },
                        "sha1" : { "type" : "string", "index" : "not_analyzed" },
                        "source" : { "type" : "string", "index" : "not_analyzed" },
                        "startingPage" : { "type" : "string", "index" : "not_analyzed" },
                        "time" : {"type" : "date"},
                        "title" : { "type" : "string"},
                        "contents": {"type": "string", "analyzer" : "english",
                            "fields": {
                                "case_sensitive": {"type": "string", "analyzer": "case_sensitive"},
                                "no_stem": {"type": "string", "analyzer": "no_stem"},
                                "case_sensitive_no_stem": {"type": "string", "analyzer": "case_sensitive_no_stem"}
                                }
                            },
                        "vol" : { "type" : "string", "index" : "not_analyzed" }
                        }
                    },
                "queries": {
                    "properties": {
                        "query": {
                            "type": "percolator"
                        }
                    }
                }
                }
        self.es.indices.close(index=index_name)
        self.es.indices.put_mapping(self.es_type, body=mapping, index=index_name, timeout="30s")
        self.es.indices.open(index=index_name)
        return 0

    def create_index(self, index_name):
        """
        Create an Elasticsearch index with the fields, analyzers, and mappings
        that have proven to be useful. Analyzes the "contents" field (extracted
        text layer) using an english language analyzer (with and without case
        sensitivity) along with an unstemmed index (also with and without case
        sensitivity)

        Args:
            index_name (string): The name of the index to create.

        Returns: 0
        """
        if not self.es.indices.exists(index_name):
            settings =  {
                    "number_of_shards" : 25,
                    "refresh_interval" : "10s",
                    "analysis" : {
                       "filter": {
                           "english_stop": {
                                "type":       "stop",
                                "stopwords":  "_english_"
                                },
                            "english_stemmer": {
                                "type":       "stemmer",
                                "language":   "english"
                                },
                            "english_possessive_stemmer": {
                                "type":       "stemmer",
                                "language":   "possessive_english"
                                }
                            },
                        "analyzer" : {
                            "english": {
                                "type": "custom",
                                "tokenizer":  "standard",
                                "filter": [
                                    "english_possessive_stemmer",
                                    "lowercase",
                                    "english_stop",
                                    "english_stemmer"
                                    ]
                                },
                            "case_sensitive" : {
                                "type": "custom",
                                "tokenizer":  "standard",
                                "filter": [
                                    "english_possessive_stemmer",
                                    "english_stop",
                                    "english_stemmer"
                                    ]
                                },
                            "no_stem" : {
                                "type": "custom",
                                "tokenizer":  "standard",
                                "filter": [
                                    "lowercase",
                                    "english_stop"
                                    ]
                                },
                            "case_sensitive_no_stem" : {
                                "type": "custom",
                                "tokenizer":  "standard",
                                "filter": [
                                    "english_stop"
                                    ]
                                }
                            }
                        }
                    }
            self.es.indices.create(index=index_name, body=settings)
            print "Waiting for ok status..."
            self.es.cluster.health(wait_for_status="yellow")
            self.update_mapping(index_name)
            return 0

    def doc_exists(self, doc_id, source_index = None, attempts=0):
        """
        Check if a document exists in a given index. If an index is specified,
        it will check that one. Otherwise it defaults to the ESHelper's default.

        Args:
            doc_id (string) : Document ID
            source_index (string): ES Index to check
            attempts (int): How many times this call has been attempted --
            useful for retries when ES is performing poorly

        Returns:
            True if the document is found, otherwise false.
        """
        url = "%s/%s" % (self.es_type_url, doc_id)
        url = url.replace(self.index, source_index) if source_index is not None else url
        try:
            req = requests.get(url).json()
            return req["found"]
        except (ValueError, KeyError, elasticsearch.exceptions.ConnectionTimeout):
            if attempts <= 3:
                time.sleep(60)
                return self.doc_exists(doc_id, source_index, attempts+1)
            else:
                return False
        return False

    def percolate_doc(self, doc_id, source_index=None):
        """
        Percolate an existing document, by its internal ID

        Args:
            doc_id (string) : Internal ID (ObjectId as assigned by mongoDB)
            source_index (string) : The ES index to check against. If not specified,
            the ESHelper's default index is utilized

        Returns:
            matching_queries -- a list of query ids found in the document.
        """

        url = "%s/%s/%s/%s/_percolate" % (self.es_url, self.index, "article", doc_id)
        req = requests.get(url)
        matching_queries = []
        if req.status_code == 200:
            for i in req.json()["matches"]:
                matching_queries.append(i["_id"])
        else:
            print "Error in percolation! Retrying a few times! Status code: %s" % req.status_code
            print req.text
            attempts = 0
            status_code = None
            while attempts < 3 and status_code!=200:
                attempts+=1
                print "Attempt %s" % attempts
                print "\t%s" % status_code
                req = requests.get(url)
                status_code = req.status_code
                if status_code == 200:
                    print "Success now!"
                    for i in req.json()["matches"]:
                        matching_queries.append(i["_id"])
        return matching_queries

    def scan_and_scroll(self, query, size=200, limit=9999999999999):
        """
        Scan-and-scroll Elasticsearch query -- will compile and return the full
        set of results matching the specified query. Useful for very large
        responses.

        Args:
            query (dict) : The ES query.
            size (int) : How many documents to return for each iteration of the scroll.

        Returns:
            hits -- a list of the returned documents that match the query

        """
        hits = []
        try:
            query["size"] = size
            scan = self.es.search(index=self.index, doc_type=self.es_type, \
                    request_timeout=300, body=query,
                scroll = "10m"
                )
            scroll_id = scan["_scroll_id"]
            while (len(hits) < scan["hits"]["total"]) and (len(hits) < limit):
                scroll = self.es.scroll(scroll_id = scroll_id, scroll='10m', request_timeout=300)
                scroll_id = scroll["_scroll_id"]
                hits += scroll["hits"]["hits"]
            if scan["hits"]["total"] != len(hits) and len(hits) < limit:
		print "ERROR! %s hits found, but expected %s or %s" % (len(hits), scan["hits"]["total"], limit)
		return []
        except elasticsearch.exceptions.TransportError:
            print "Transport error while scan/scrolling! Waiting and retrying, with size %s this time." % str(int(size/5+1))
            time.sleep(60)
            hits = self.scan_and_scroll(query, size=int(size/5 + 1)) # totally arbitrary to see if the issue might be grabbing too many docs at once

	print "Returning hits len %s" % len(hits)
        return hits

    def search_term_from_date(self, ext_str, from_date, field="contents", classification=[], size=200):
        """
        Search for matching docs (to be used, among other things, when adding a new term)

        Args:
            ext_str (string) : The string term to search for.
	    from_date (datetime) : Match only articles since this date.
            field (string) : The ES field to search (default: contents)
            classification (list) : The additional terms to search as a
            hierarchy check.  The search will be for the ext_str PLUS any of
            the terms in the classification list.
            size (int) : How many documents to return for each iteration of the scroll.

        Returns:
            matches (dict) : {docid : n}, where n is the number of times that
                term appears in the document

        """
        matching_docs = {}
        if classification == []:
	    query = {
		"bool": {
		    "must": [
			{"match_phrase" : {field : ext_str}},
			{
			    "range" : {
				"time" : {
				    "gte" : from_date.strftime("%m/%d/%Y"),
				    "format": "MM/dd/yyyy"
				    }
				}
			    }
			]
		    }
		}
        else: # look for other terms in the classification hierarchy
            query = {"bool": {
                "must": [
                    {"match_phrase": {
                        field: ext_str
                        }
                        },
                    {
                        "bool" : { # look for additional terms from classification
                            "should" : [{"match_phrase" : {field : term}} for term in classification if term != ext_str]
                        }
                    },
                    {
                        "range" : {
                            "time" : {
                                "gte" : from_date.strftime("%m/%d/%Y"),
                                "format": "MM/dd/yyyy"
                                }
                            }
                        }
                ]
                }
            }
        try:
            scan = self.es.search(index=self.index, doc_type=self.es_type, \
                    request_timeout=300, body= \
                {
                    "query": query,
                    "fields" : ["_id", "_explanations", "time"],
                    "explain" : True,
                    "size": size
                },
                scroll = "10m"
                )
            scroll_id = scan["_scroll_id"]
            start_time = time.time()
            m = re.compile(r"'(?:phraseFreq|termFreq)=(\d+).0'")
            while (len(matching_docs) < scan["hits"]["total"]):
                scroll = self.es.scroll(scroll_id = scroll_id, scroll='10m', request_timeout=300)
                scroll_id = scroll["_scroll_id"]
                for doc in scroll["hits"]["hits"]:
                    # for each doc scan the query explanation, which includes the number of times a term/phrase appears
                    explanation = str(doc["_explanation"])
                    result = m.findall(explanation)
                    try:
                        matching_docs[doc["_id"]] = int(result[0])
                    except:
                        print "Error in getting n_hits for this document!"
                        return {}
            if VERBOSE: print "%s scrolled for %s (%.2f per sec)" % (scan["hits"]["total"], ext_str, float(scan["hits"]["total"])/float(time.time() - start_time))
            if scan["hits"]["total"] != len(matching_docs): return {}
        except elasticsearch.exceptions.TransportError:
            print "Transport error while scan/scrolling! Waiting and retrying, with size %s this time." % str(int(size/5+1))
            time.sleep(60)
            matching_docs = self.search_term_from_date(ext_str, from_date, field, size=int(size/5 + 1)) # totally arbitrary to see if the issue might be grabbing too many docs at once

        return matching_docs

    def search_new_term(self, ext_str, field="contents", classification=[], size=200):
        """
        Search for matching docs (to be used, among other things, when adding a new term)

        Args:
            ext_str (string) : The string term to search for.
            field (string) : The ES field to search (default: contents)
            classification (list) : The additional terms to search as a
            hierarchy check.  The search will be for the ext_str PLUS any of
            the terms in the classification list.
            size (int) : How many documents to return for each iteration of the scroll.

        Returns:
            matches (dict) : {docid : n}, where n is the number of times that
                term appears in the document

        """
        print "Searching for %s in %s" % (ext_str, field)
        matching_docs = {}
        if classification == []:
            query = {"match_phrase": {field: ext_str}}
        else: # look for other terms in the classification hierarchy
            query = {"bool": {
                "must": [
                    {"match_phrase": {
                        field: ext_str
                        }
                        },
                    {
                        "bool" : { # look for additional terms from classification
                            "should" : [{"match_phrase" : {field : term}} for term in classification if term != ext_str]
                        }
                    }
                ]
                }
            }
        try:
            print "Trying a query"
            print self.index, self.es_type
            print query
            scan = self.es.search(index=self.index, doc_type=self.es_type, \
                    request_timeout=300, body= \
                {
                    "query": query,
                    "stored_fields" : ["_id", "_explanations"],
                    "explain" : True,
                    "size": size
                },
                scroll = "10m"
                )
            m = re.compile(r"'(?:phraseFreq|termFreq)=(\d+).0'")
            for doc in scan["hits"]["hits"]:
                explanation = str(doc["_explanation"])
                result = m.findall(explanation)
                matching_docs[doc["_id"]] = int(result[0])
            scroll_id = scan["_scroll_id"]
            start_time = time.time()
            print "Found %s hits" % scan["hits"]["total"]
            while (len(matching_docs) < scan["hits"]["total"]):
                scroll = self.es.scroll(scroll_id = scroll_id, scroll='10m', request_timeout=300)
                scroll_id = scroll["_scroll_id"]
                for doc in scroll["hits"]["hits"]:
                    # for each doc scan the query explanation, which includes the number of times a term/phrase appears
                    explanation = str(doc["_explanation"])
                    result = m.findall(explanation)
                    try:
                        matching_docs[doc["_id"]] = int(result[0])
                    except:
                        print "Error in getting n_hits for this document!"
                        return {}
            if VERBOSE: print "%s scrolled for %s (%.2f per sec)" % (scan["hits"]["total"], ext_str, float(scan["hits"]["total"])/float(time.time() - start_time))
            if scan["hits"]["total"] != len(matching_docs): return {}
        except elasticsearch.exceptions.TransportError:
            print sys.exc_info()
            print "Transport error while scan/scrolling! Waiting and retrying, with size %s this time." % str(int(size/5+1))
            time.sleep(60)
            matching_docs = self.search_new_term(ext_str, field, size=int(size/5 + 1)) # totally arbitrary to see if the issue might be grabbing too many docs at once

        return matching_docs

    def does_percolator_exist(self, dict_name, term):
        """
        Checks to see if a dictionary, term combination has been saved as a
            query (as a percolator).

        Args:
            dict_name (string): dictionary to check
            term (string): dictionary term to check

        Returns:
            true if found, false if not
        """
        url = "%s/%s/.percolator/%s_%s" % (self.es_url, self.index, dict_name, term)
        resp = requests.get(url)
        if resp.status_code == 200:
            if resp.json()["found"]:
                return True
            else:
                return False
        else:
            return False


    def add_percolator(self, parent_id, ext_string, field, classification = []):
        """
        Add a percolator for the given external string object (plus
        classification object). Adds a percolator object to Elasticsearch and
        updates (or adds) the matching row in the query table.

        Args:
            parent_id (string): Name of source for bookkeeping (e.g. "paleobiodb" or "macrostrat")
            ext_string (string): The external string value to store
            field (string) : The Elasticsearch indexed field to search
            classification (list) : The additional terms to search as a
            hierarchy check.  The search will be for the ext_str PLUS any of
            the terms in the classification list.

        Returns:
            0 if successful, otherwise 1
        """

        url = "%s/%s/queries/%s_%s" % (self.es_url, self.index, parent_id, ext_string.replace("/", "|"))
        if "found" in requests.get(url).json() and requests.get(url).json()["found"]: # already exists -- no need to create again
            if VERBOSE:
                print "Percolator found! Skipping. %s" % url
            return 0
        else:
            if classification == []:
                query = {"match_phrase": {field: ext_string}}
            else: # look for other terms in the classification hierarchy
                query = {"bool": {
                    "must": [
                        {"match_phrase": {
                            field: ext_string
                            }
                            },
                        {
                            "bool" : { # look for additional terms from classification
                                "should" : [{"match_phrase" : {field : term}} for term in classification if term != ext_string]
                            }
                        }
                    ]
                    }
                }
            body = {"query":query,
                    "highlight":
                        {"stored_fields":
                            {field.replace(".", "*") :{
                                "fragment_size" : 10,
                                "number_of_fragments": 9999999,
                                "highlight_query" : {"match_phrase" : {field.replace(".", "*"): ext_string}}
                                }
                            }
                        }
                    }
            print "Adding percolator at %s" % url
            req = requests.post(url, data=json.dumps(body))
            if req.status_code == 200 or req.status_code == 201:
                return 0
            else:
                print "Could not add percolator! HTTP response code: %s" % req.status_code
                print "URL: %s" % url
                return 1


    def remove_percolator(self, parent_id, ext_string):
        """
        Removes a percolator for the given external string object. Removes the
        percolator object to Elasticsearch and deletes the matching row in the
        query table

        Args:
            parent_id (string) : Name of source for bookkeeping (e.g. "paleobiodb" or "macrostrat")
            ext_string (string): The external string value to store

        Returns:
            0 if successful, otherwise 1

        """
        url = "%s/%s/.percolator/%s_%s" % (self.es_url, self.index, parent_id, ext_string.replace("/", "|"))
        print url
        req = requests.get(url)
        if req.status_code == 200:
            if req.json():
                if "found" in requests.get(url).json() and requests.get(url).json()["found"]: # found a matching percolator
                    requests.delete(url)
                    print "Deleted %s" % url
                else:
                    return 1
        else:
            return 1
        return 0
