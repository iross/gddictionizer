#!/usr/bin/env python
# encoding: utf-8

import GddConfig
import ElasticsearchHelper
import elasticsearch
import glob
import os

esh = ElasticsearchHelper.ElasticsearchHelper(es_index="temp_articles", es_type="article")

def match_term_to_docs(term, field, dict_id = None, classification=[], from_date=None):
    """
    Use Elasticsearch to match a term to docids and insert into `terms_docs`
    If classification!=[], two searches will be run. The (docid, term) row
    in the `terms_docs` table will indicate whether the additional terms
    were found in the document

    Args:
        term (string): a term from a dictionary
        field (strimg): The contents field to use (e.g. contents.case_sensitive_word_stemming)
        dict_id (int): The primary ID of the dictionary to match against
        classification (list): list of additional terms that should be searched in the document
    """
    # match twice -- once with hierarchy, once without.
    if from_date is None:
        doc_matches_no_classification = esh.search_new_term(term, field, [], size=200)
    else:
        doc_matches_no_classification = esh.search_term_from_date(term, from_date, field, [], size=200)
    if classification != []: # if there are addtl terms to consider, run another search
        if from_date is None:
            doc_matches = esh.search_new_term(term, field, classification, size=200)
        else:
            doc_matches = esh.search_term_from_date(term, from_date, field, classification, size=200)

for doc in glob.glob("/input/*.txt"):
    with open(doc) as fin:
        text = fin.read()
    docid = os.path.basename(doc).replace(".txt", "")
    doc = {"contents" : text}
    esh.es.index(index="temp_articles", doc_type="article", id=docid, body=doc)
    print "Inserting %s" % doc

with open("/input/terms") as fin:
    for term in fin:
        # TODO: parse hierarchy and do things with it
        res = esh.search_new_term(term.strip(), "contents", classification=[], size=200)
        with open("/output/terms_docs", "a") as fout:
            for docid, hits in res.items():
                fout.write("%s,%s,%s\n" % (term, docid, hits))
