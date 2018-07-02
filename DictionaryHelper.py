import os, sys
if "__pypy__" in sys.builtin_module_names:
    import psycopg2cffi as psycopg2
else:
    import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs
import requests
import json
import GddConfig
import codecs
import ElasticsearchHelper
from datetime import datetime

try:
    from settings import *
except ImportError:
    print "Could not import settings!"
    sys.exit(1)

VERBOSE = False

class DictionaryHelper(object):
    """
    Methods for managing term dictionaries
    """

    def __init__(self, corpus = "DEFAULT"):
        self.corpus = corpus
        config = GddConfig.GddConfig()
        self.config = config

        self.pg_connection = psycopg2.connect(dbname=config.get("psql_dbname", self.corpus), user=config.get("psql_user", self.corpus), password=config.get("psql_password", self.corpus), host=config.get("psql_host", self.corpus), port=config.get("psql_port", self.corpus))

        self.pg_connection.autocommit = True
        self.pg_cursor = self.pg_connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)


    def __delete__(self):
        self.pg_cursor.close()
        self.pg_connection.close()

    def list_terms(self, dict_name):
        """
        List terms for a dictionary.

        Args:
            dict_name (string): Name of dictionary of interest.

        Returns: List of terms for the given dictionary.

        """
        terms = []
        self.pg_cursor.execute("""
          SELECT dict_id FROM dictionaries WHERE name ilike %(name)s;
          """, {"name" : dict_name})
        dict_id = self.pg_cursor.fetchone()['dict_id']
        self.pg_cursor.execute("""
            SELECT term FROM dict_terms
          WHERE dict_id = %(dict_id)s;
          """, {"dict_id" : dict_id})
        terms = [i['term'] for i in self.pg_cursor.fetchall()]
        return terms

    def compare_stored_live(self, term, dict_id, field = "contents", fix = False):
        """
        Compare the count of matches in the stored postgres tables
        to the count of matching documents from a live Elasticsearch query.

        If all percolators are working as expected, there should be no difference.

        Args:
            term (string): Term to compare
            dict_id (int): The primary ID of the dictionary to check
            field (string): Elasticsearch field to query (e.g. contents or contents.case_sensitive)
            fix (bool): If true, re-match the term to get stored and live searching synchronized.

        Returns: tuple of (number of stored documents with match, number of live documents that matches)
        """
        n_docs_stored = 0
        n_docs_live = 0
        resp = requests.get("http://deepdivesubmit.chtc.wisc.edu/api/terms?term=%s" % term)
        if resp.status_code == 200:
            try:
                if resp.json()["success"]["data"][0] is not None:
                    n_docs_stored = resp.json()["success"]["data"][0]["n_docs"]
            except TypeError:
                print "Error in query!"
                return (0, 0)

        inner = {field : term}

        es_helper = ElasticsearchHelper.ElasticsearchHelper(self.config.get("es_index", self.corpus))

        query = {
                "query" :
                    {"bool" :
                        {"must" : {"match_phrase" : inner}}
                    }
                }
        esresp = es_helper.es.search(index=self.config.get("es_index", self.corpus), body=query)
        n_docs_live = esresp["hits"]["total"]
        if fix:
            self.match_term_to_docs(term, field, dict_id)
        del es_helper
        return (n_docs_stored, n_docs_live)

    def check_dictionary_freshness(self, dict_name, fix=False):
        """
        Checks the "freshness" of a dictionary (i.e. whether or not the postgres
        tables are up to date with the articles in the Elasticsearch index)

        Args:
            dict_name (string): Name of dictionary to check.
            fix (bool): If true, re-match the terms in the dictionary to get \
            stored and live searching synchronized.

        Returns: True, if all terms in the dictionary have equal number of matches in the
        stored and live data stores. Else, False.

        """

        dict_id = self.get_dictionary_metadata(dict_name)["dict_id"]
        terms = self.list_terms(dict_name)

        n_looks_good = 0
        n_looks_bad = 0

        field = self.get_contents_field(dict_name)

        for term in terms:
            n_docs_stored, n_docs_live = self.compare_stored_live(term, dict_id, field, fix=fix)

            if n_docs_live <= n_docs_stored:
                n_looks_good += 1
            else:
                n_looks_bad += 1
                print "%s\t%s stored\t%s live" % (term, n_docs_stored, n_docs_live)

        print "%s look fine." % n_looks_good
        print "%s have differing n_matches." % n_looks_bad
        if n_looks_bad > 0:
            return False
        return True

    def list_dicts(self, verbose):
        """
        Simply list available dictionaries

        Args:
            verbose (bool): Optionally return all fields

        Returns:
            postgresql cursor pointing to the result
        """
        if verbose:
            self.pg_cursor.execute("""
            SELECT * FROM dictionaries ORDER BY dict_id
            """)
        else:
            self.pg_cursor.execute("""
            SELECT dict_id, name, last_updated
            FROM dictionaries
            ORDER BY last_updated DESC
            """)

        return self.pg_cursor.fetchall()

    def get_contents_field(self, dict_name):
        """
        Lookup the settings for the passed dictionary name and return the
        string of the contents field that it needs to use.
        (e.g. "pbdb" is case_sensitive + shouldn't use stemming, so
        it should use the contents.case_sensitive_word_stemming field)

        Args:
            dict_name (string): The dictionary name to lookup

        Returns:
            field (string): The field containing the relevant contents field.

        """
        field = None
        dict_metadata = self.get_dictionary_metadata(dict_name)
        if dict_metadata["case_sensitive"]:
            if dict_metadata["word_stemming"]:
                field = "contents.case_sensitive"
            else:
                field = "contents.case_sensitive_no_stem"
        else:
            if dict_metadata["word_stemming"]:
                field = "contents"
            else:
                field = "contents.no_stem"
        return field

    def remove_percolators(self, dict_name):
        """
        Remove percolators. Good for re-adding if they're screwed up somehow.

        Args:
            dict_name (string): The dictionary name to lookup

        Returns: 0
        """
        # get dict_id + terms
        # Make sure the dictionary exists
        self.pg_cursor.execute("""
          SELECT dict_id FROM dictionaries WHERE name ilike %(name)s;
          """, {"name" : dict_name})
        dict_id = self.pg_cursor.fetchone()['dict_id']
        self.pg_cursor.execute("""
            SELECT term FROM dict_terms
          WHERE dict_id = %(dict_id)s;
          """, {"dict_id" : dict_id})
        temp_terms = self.pg_cursor.fetchall()
        terms = [i['term'] for i in temp_terms]

        es_helper = ElasticsearchHelper.ElasticsearchHelper(self.config.get("es_index", self.corpus))

        for term in terms:
            print "Removing %s" % term
            es_helper.remove_percolator(dict_id, term)

        return 0

    def remove(self, dict_name):
        """
        Remove a dictionary and all associated entities (terms matched, percolator, subsets)

        Args:
            dict_name (string): Name of dictionary to remove
        Returns:
            0
        """
        # get dict_id + terms
        # Make sure the dictionary exists
        self.pg_cursor.execute("""
          SELECT dict_id FROM dictionaries WHERE name ilike %(name)s;
          """, {"name" : dict_name})
        check = self.pg_cursor.fetchone()
        if check is None:
            return 1
        else:
            dict_id = check['dict_id']
        self.pg_cursor.execute("""
            SELECT term FROM dict_terms
          WHERE dict_id = %(dict_id)s;
          """, {"dict_id" : dict_id})
        temp_terms = self.pg_cursor.fetchall()
        terms = [i['term'] for i in temp_terms]

        es_helper = ElasticsearchHelper.ElasticsearchHelper(self.config.get("es_index", self.corpus))

        for term in terms:
            es_helper.remove_percolator(dict_id, term)
            # remove from dict_terms
            self.delete_term(dict_id, term)

        # delete dict_subset
        for nlp_type in NLP_TYPES:
            self.pg_cursor.execute("""
              DROP TABLE IF EXISTS dict_subsets.%(table_name)s;

              DELETE FROM dict_subsets.meta
              WHERE dict_id = %(dict_id)s
              AND subset_name = %(dict_name)s;

            """, {
              "table_name": AsIs(dict_name + "_" + nlp_type),
              "dict_id": dict_id,
              "dict_name": dict_name
            })

        # remove from dictionaries
        self.pg_cursor.execute("""
          DELETE FROM dictionaries WHERE name = %(dict_name)s
        """, {
          "dict_name": dict_name
        })

        self.pg_connection.commit()

        return 0

    def add_dict_metadata(self, dictionary):
        """
        Store a dictionary's metadata.

        Args:
            dictionary (dict): {"source", "name", "base_classification", \
            "case_sensitive", "word_stemming", "key", "json_path", \
            "classification_path"}
        """
        # Make sure we have all required fields
        print dictionary
        if not "name" in dictionary or not "source" in dictionary or not "base_classification" in dictionary:
            print "A name, source, and base_classification are required"
            return

        # Check if this dictionary already exists
        self.pg_cursor.execute("""
          SELECT dict_id
          FROM dictionaries
          WHERE name ilike concat('%%', %(name)s, '%%')
             OR source ilike concat('%%', %(source)s, '%%')
             OR base_classification ilike concat('%%', %(base_classification)s, '%%')
        """, {
          "name": dictionary["name"],
          "source": dictionary["source"],
          "base_classification": dictionary["base_classification"]
        })

        check = self.pg_cursor.fetchone()
        if check is not None:
            print "This dictionary already exists (id %s)" % check['dict_id']
            key_in = raw_input("Enter Y to proceed anyway. Any other key exits.")
            if key_in.lower() != "y":
                return

        # Insert the new dictionary
        self.pg_cursor.execute("""
          INSERT INTO dictionaries (%(fields)s)
          VALUES %(data)s
        """, {
          "fields": AsIs(", ".join(dictionary.keys())),
          "data": tuple([dictionary[key] for key in dictionary.keys()])
        })
        self.pg_connection.commit()



    def subset(self, dict_name):
        """
        Refresh/create the subsets for a given dictionary

        Args:
            dict_name (string): the name of a dictionary
        """

        # Make sure the dictionary exists
        self.pg_cursor.execute("""
          SELECT dict_id
          FROM dictionaries
          WHERE name ilike %(name)s
        """, {
          "name": dict_name
        })

        dict_subset_meta = self.pg_cursor.fetchone()

        if dict_subset_meta is None:
            print "Dictionary name not found"
            return


        # Create new subset tables
        for nlp_type in NLP_TYPES:
            self.pg_cursor.execute("""
              DROP TABLE IF EXISTS dict_subsets.%(table_name)s;

              CREATE TABLE dict_subsets.%(table_name)s AS
              SELECT
                s.docid,
                s.sentid
              FROM sentences_%(nlp_type)s s

              JOIN (
              	SELECT DISTINCT docid
              	FROM terms_docs
                JOIN dict_terms ON terms_docs.term = dict_terms.term
              	WHERE terms_docs.dict_id = %(dict_id)s
              ) AS temp ON temp.docid = s.docid
              ORDER BY docid, sentid;

              CREATE INDEX ON dict_subsets.%(table_name)s (docid);

              DELETE FROM dict_subsets.meta
              WHERE dict_id = %(dict_id)s
              AND subset_name = '%(table_name)s';

              INSERT INTO dict_subsets.meta (dict_id, subset_name)
              VALUES (%(dict_id)s, '%(table_name)s');
            """, {
              "table_name": AsIs(dict_name + "_" + nlp_type),
              "nlp_type": AsIs(nlp_type),
              "dict_id": dict_subset_meta["dict_id"]
            })

        self.pg_connection.commit()
        print 'Done subsetting ', dict_name

    def overlapping_subset(self, dict_1, dict_2, table_name, terms_1=[], terms_2=[]):
        """
        Creates a docid:sentid table from the overlap between two dictionaries
        OR two sets of terms.  e.g. documents that contain one or more words
        from set A AND one or more words from set B

        NOTE: if "terms_1" or "terms_2", the entire collection of words from
        dict_1 and dict_2 are used.
        NOTE: THERE IS NO EXPLICIT CHECK TO SEE IF PASSED TERMS ARE REALLY IN
        THE DICTS SPECIFIED.  (so it's possible to build overlaps between
        arbitrary terms, so long as they've been indexed somewhere)


        Args:
            dict_1 (string): First dictionary.
            dict_2 (string): Second dictionary.
            table_name (string): Name of the output table
            terms_1 (list): Terms to include from the first dictionary (default: all terms used)
            terms_2 (list): Terms to include from the second dictionary (default: all terms used)
        """
        if terms_1 == []: # assume we want all terms from dict_1
            terms_1 = self.list_terms(dict_1)
        if terms_2 == []: # assume we want all terms from dict_2
            terms_2 = self.list_terms(dict_2)

        for nlp_type in NLP_TYPES:
            self.pg_cursor.execute("""
              DROP TABLE IF EXISTS dict_subsets.%(table_name)s;

              CREATE TABLE dict_subsets.%(table_name)s AS
              SELECT s.docid, s.sentid FROM sentences_%(nlp_type)s s JOIN
                  (SELECT DISTINCT(a.docid) FROM terms_docs AS a INNER JOIN
                    (SELECT docid, term FROM terms_docs WHERE term=ANY(%(terms_1)s)) AS b
                        ON a.docid=b.docid
                    WHERE a.term=ANY(%(terms_2)s)) AS temp ON temp.docid=s.docid
                ORDER BY docid, sentid;

              DELETE FROM dict_subsets.meta
              WHERE subset_name = '%(table_name)s';

              INSERT INTO dict_subsets.meta (dict_id, subset_name)
              VALUES (%(dict_id)s, '%(table_name)s');
            """, {
                "terms_1" : terms_1,
                "terms_2" : terms_2,
                "table_name": AsIs(table_name + "_" + nlp_type),
                "nlp_type": AsIs(nlp_type),
                "dict_id": 0
            })

        self.pg_connection.commit()
        print 'Done finding an overlapping subset  -- %s created' % table_name

    def insert_into_dict_terms(self, dictid, term, classification):
        """
        Updates the table dict_terms

        Args:
            dictid (int): the unique dictionary identifier
            term (string): the term to be inserted
            classification (list): an array of terms that dictate hierarcy of the given term

        Effects:
            Adds a row to the dict_terms table

        """

        # Insert the dictionary / term / classification tuple
        self.pg_cursor.execute("""
          INSERT INTO dict_terms (dict_id, term, classification)
          VALUES (%(dictid)s, %(term)s, %(classification)s)
        """, {
            "dictid": dictid,
            "term": term,
            "classification": classification
        })
        self.pg_connection.commit()


    def get_dictionary_metadata_by_id(self, dict_id):
        """
        Get metadata pertaining to a dictionary.

        Args:
            dict_id (string): Dict_id of the dictionary of interest

        Returns: dict containing the dictionary's metadta (dict_id, name, source, case_sensitive, etc)

        """
        self.pg_cursor.execute("""
            SELECT dict_id, name, source, json_path, key, base_classification, classification_path, case_sensitive, users, word_stemming
            FROM dictionaries
            WHERE dict_id = %(dict_id)s
        """,
        {"dict_id": dict_id}
        )
        return self.pg_cursor.fetchone()

    def get_dictionary_metadata(self, dict_name):
        """
        Get metadata pertaining to a dictionary.

        Args:
            dict_name (string): Dictionary of interest

        Returns: dict containing the dictionary's metadta (dict_id, name, source, case_sensitive, etc)

        """
        self.pg_cursor.execute("""
            SELECT dict_id, name, source, json_path, key, base_classification, classification_path, case_sensitive, users, word_stemming
            FROM dictionaries
            WHERE name = %(name)s
        """,
        {"name": dict_name}
        )
        return self.pg_cursor.fetchone()

    def health_check(self, dict_name=None, fix=False):
        """
        Do a health check against the passed dictionary. If no dictionary is
        specified, run a health check against all of them.  The health check
        includes:
                * Ensure percolators exist + work
                * Ensure all terms with matches have proper entries in terms_docs
                * Check latest match/subset date (and encourage rerunning if the data looks stale)

        Args:
            dict_name (string): Name of dictionary to check. If None, check all dictionaries.
            fix (bool): If True, it will attempt to fix the errors.

        Returns: True if it looks healthy, otherwise False

        """
        if dict_name is not None:
            dicts = [dict_name]
        else:
            self.pg_cursor.execute("""SELECT name FROM dictionaries;""")
            dicts = [i["name"] for i in self.pg_cursor.fetchall()]

        es_helper = ElasticsearchHelper.ElasticsearchHelper(self.config.get("es_index", self.corpus))

        healthy = True
        for dictionary in dicts:
            dictionary_meta = self.get_dictionary_metadata(dictionary)

            if dictionary_meta is None:
                print "Dictionary ", dict_name, " not found"
                sys.exit(1)

            print "--- %s ---" % dictionary
            self.pg_cursor.execute("""SELECT term
                     FROM dict_terms
                       WHERE term NOT IN (
                                   SELECT distinct term from terms_docs
                                            )
                          AND dict_id = (SELECT dict_id FROM dictionaries WHERE name ilike %(name)s)
                              ORDER BY term;""", {"name" : dict_name})
            # get terms
            terms = [i["term"] for i in self.pg_cursor.fetchall()]

            field = self.get_contents_field(dict_name)
            print "Checking percolators..."
            for term in terms:
                # check for percolator existence
                check = es_helper.does_percolator_exist(dictionary_meta["dict_id"], term)
                if check:
                    continue
                else:
                    healthy = False
                    print "Missing percolator! Term: %s" % term
                    if fix:
                        es_helper.add_percolator(dictionary_meta["dict_id"], term, field)
                # check if percolator works?

            print "Checking match counts for a random subsample..."
            # check random sample of terms
            self.pg_cursor.execute("""SELECT term
                     FROM dict_terms
                          WHERE dict_id = (SELECT dict_id FROM dictionaries WHERE name ilike %(name)s)
                          AND random() < 0.1 LIMIT 1000;""", {"name" : dict_name})
            terms_sample = [i["term"] for i in self.pg_cursor.fetchall()]
            mismatched_terms = 0
            for term in terms_sample:
                stored_docs = self.pg_cursor.execute("""SELECT DISTINCT(docid) FROM terms_docs WHERE term=%(term)s;""" ,{"term": term})
                stored_docs = [i['docid'] for i in self.pg_cursor]
                # do 'live' check
                current_matches = es_helper.search_new_term(term, field)
                if len(current_matches) != len(stored_docs):
                    mismatched_terms+=1
                    healthy = False
                    print "Document-level matches missing from term %s! (stored: %s, current: %s)" % (term, len(stored_docs), len(current_matches))
                if fix:
                    self.match_term_to_docs(term, field)
            print "%s of %s checked terms had disagreement in stored vs current matches." % (mismatched_terms, len(terms_sample))

        return healthy

    def delete_term(self, dict_id, term, classification=[]):
        """
        Updates the tables dict_terms, term_docs, and terms_docs_sentences

        Args:
            dictid (int): the unique dictionary identifier
            term (string): the term to be deleted
        """
        # Log it
        with codecs.open("log.txt", "a", "utf-8") as fout:
          fout.write("Removing row from %s -- (%s, %s)\n" % (dict_id, term, datetime.now()))

        # Delete from terms_docs
        self.pg_cursor.execute("""
          DELETE FROM terms_docs WHERE term = %(term)s AND dict_id=%(dict_id)s
        """, {
          "term": term,
          "dict_id" : dict_id
        })

        # ensure that there are no other dictionaries that include the term before
        # deleting from terms_docs_sentences (since we don't utilize dict_id there)
        self.pg_cursor.execute("""
                SELECT COUNT(*) FROM dict_terms WHERE term=%(term)s AND dict_id=%(dict_id)s;
                """, {"term": term, "dict_id" : dict_id})

        count = self.pg_cursor.fetchone()['count']

        if count==1:
            # Delete from terms_docs_sentences
            self.pg_cursor.execute("""
              DELETE FROM terms_docs_sentences WHERE term = %(term)s
            """, {
              "term": term
            })

        # Delete from dict_terms
        self.pg_cursor.execute("""
          DELETE FROM dict_terms WHERE dict_id = %(dictid)s AND term = %(term)s AND classification=%(classification)s
        """, {
          "dictid": dict_id,
	  "classification" : classification,
          "term": term
        })
        self.pg_connection.commit()


    def insert_into_terms_docs_bulk(self, bulk):
        """
        """
        # Upsert the term / docid / hits
#        $1: hits $2: last_updated $3: hierarchy_present $4: from_perc $5: dict_id $6:term $7: docid
        self.pg_cursor.execute(""" PREPARE stmt AS
          WITH upsert AS (
            UPDATE terms_docs SET hits = $1, last_updated = $2, hierarchy_present = $3, from_percolation = $4, dict_id = $5
            WHERE dict_id = $5 AND term = $6 AND docid = $7
            RETURNING *
          )
          INSERT INTO terms_docs (term, docid, hits, last_updated, hierarchy_present, dict_id, from_percolation)
          SELECT $6, $7, $1, $2, $3, $5, $4
          WHERE NOT EXISTS (SELECT * FROM upsert)
                """)
        psycopg2.extras.execute_batch(self.pg_cursor, "EXECUTE stmt (%s,%s,%s,%s,%s,%s,%s)", [(i["hits"], i["last_updated"], i["hierarchy_present"], i["from_percolation"], i["dict_id"], i["term"], i["docid"]) for i in bulk], page_size=100)
        self.pg_cursor.execute("DEALLOCATE stmt")
        self.pg_connection.commit()

    def insert_into_terms_docs(self, term, docid, hits, hierarchy_present, dict_id, from_percolation = False):
        """
        Update the table terms_docs

        Args:
            term (string): the dictionary term to insert
            docid (string): the unique document identifier
            hits (int): number of times the term appears in the given docid
            hierarchy_present (bool): True if additional terms from the hierarchy are present in the document
            dict_id (int): The primary ID of the dictionary
            from_percolation (bool): True if the term, docid tuple was matched via percolation

        Effects:
            Changes terms_docs table

        """

        # Upsert the term / docid / hits
        self.pg_cursor.execute("""
          WITH upsert AS (
            UPDATE terms_docs SET hits = %(hits)s, last_updated = now(), hierarchy_present = %(hierarchy_present)s, from_percolation = %(from_percolation)s, dict_id = %(dict_id)s
            WHERE term = %(term)s AND docid = %(docid)s AND dict_id = %(dict_id)s
            RETURNING *
          )
          INSERT INTO terms_docs (term, docid, hits, last_updated, hierarchy_present, dict_id, from_percolation)
          SELECT %(term)s, %(docid)s, %(hits)s, now(), %(hierarchy_present)s, %(dict_id)s, %(from_percolation)s
          WHERE NOT EXISTS (SELECT * FROM upsert)
        """, {
          "term": term,
          "docid": docid,
          "hits": hits,
          "hierarchy_present": hierarchy_present,
          "dict_id" : dict_id,
          "from_percolation" : from_percolation
        })
        self.pg_connection.commit()

    def match_term_to_docs(self, term, field, dict_id = None, classification=[], from_date=None):
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
        es_helper = ElasticsearchHelper.ElasticsearchHelper(self.config.get("es_index", self.corpus))
        # match twice -- once with hierarchy, once without.
        if from_date is None:
            doc_matches_no_classification = es_helper.search_new_term(term, field, [], size=200)
        else:
            doc_matches_no_classification = es_helper.search_term_from_date(term, from_date, field, [], size=200)
        if classification != []: # if there are addtl terms to consider, run another search
            if from_date is None:
                doc_matches = es_helper.search_new_term(term, field, classification, size=200)
            else:
                doc_matches = es_helper.search_term_from_date(term, from_date, field, classification, size=200)
        # new bulk insert works easiest with a list of dicts
        to_insert = [
            {
                "term" : term,
                "docid" : doc_id,
                "hits" : hits,
                "hierarchy_present" : None if classification == [] else (True if doc_id in doc_matches.keys() else False),
                "dict_id" : dict_id,
                "from_percolation" : False,
                "last_updated" : datetime.now()
            } for doc_id, hits in doc_matches_no_classification.items()]
        self.insert_into_terms_docs_bulk(to_insert)

    def match(self, dict_name):
        """
        Match a dictionary to docs and sentences

        Args:
            dict_name (string): the name of the dictionary to be indexed
        Effects:
            Writes snippets into terms_docs_sentences
        """

        # LEFT JOIN on to terms_docs_sentences so that only term-docid pairs that aren't currently present are added
        self.pg_cursor.execute("""
          INSERT INTO terms_docs_sentences (term, docid, sentid, snippets)
          SELECT dict_terms.term, sentences_nlp.docid, sentences_nlp.sentid, array_to_string(words, ' ')
          FROM sentences_nlp
          JOIN terms_docs ON sentences_nlp.docid = terms_docs.docid
          JOIN dict_terms ON terms_docs.term = dict_terms.term
          JOIN dictionaries ON dict_terms.dict_id = dictionaries.dict_id
          LEFT JOIN terms_docs_sentences ON terms_docs_sentences.term = terms_docs.term AND terms_docs_sentences.docid = terms_docs.docid
          WHERE terms_docs_sentences.term IS NULL
            AND array_to_string(words, ' ') ~* concat('\y', dict_terms.term, '\y')
          AND dictionaries.name = %(dict_name)s;

          UPDATE dictionaries SET last_matched = now()
          WHERE name = %(dict_name)s;
        """, {
          "dict_name": dict_name
        })
        self.pg_connection.commit()
        print 'Done matching ', dict_name

    def update(self, dict_name):
        """
        Update the terms for a given dictionary source. Like ingest, but for updating.
        TODO: decide if this should just go ahead and become ingest. It's essentially
        the same but I'd rather write code than think about it as of 31.May.2018 at 3:03pm

        Args:
            dict_name (string): the name of a dictionary to update
            matches against the most recent documents.
        Effects:
            Updates `dict_terms`, Elasticsearch percolators, and matches new terms to documents
        """
        es_helper = ElasticsearchHelper.ElasticsearchHelper(self.config.get("es_index", self.corpus))

        # Fetch metadata about this dictionary
        dictionary_meta = self.get_dictionary_metadata(dict_name)

        if dictionary_meta is None:
            print "Dictionary ", dict_name, " not found"
            sys.exit(1)

        # Check if source is remote or local and load terms
        if "http" in dictionary_meta["source"]:
            source = requests.get(dictionary_meta["source"], verify=False)
            try:
                terms = source.json()
            except ValueError: # API is defined remotely, but isn't json -- likely a CSV file
                terms = []
                for line in source.text.split("\n"):
                    if line == "": continue
                    line = line.split(",")
		    terms.append([i.strip() for i in line])
                    dictionary_meta["csv"] = True
        else:
            with open(dictionary_meta["source"]) as source:
                if dictionary_meta["key"] is None: # either CSV or a one-term-per-line file
                    terms= []
                    for line in source:
                        if line == "": continue
                        line = line.split(",")
                        terms.append([i.strip() for i in line])
                        dictionary_meta["csv"] = True
                else:
                    terms = json.loads(source.read())

        # Slide down the JSON to get the list of objects
        if dictionary_meta["json_path"]:
          for key in dictionary_meta["json_path"]:
              terms = terms[key]

        # Get existing terms
        self.pg_cursor.execute("""
          SELECT term, classification
          FROM dict_terms
          WHERE dict_id = %(dict_id)s
        """, {
          "dict_id": dictionary_meta["dict_id"]
        })

        existing = set( [ (unicode(each["term"].strip(), 'utf-8'), tuple(each["classification"])) for each in self.pg_cursor.fetchall() ] )
        incoming = set()

        for idx, term in enumerate(terms): # build the set of (term, classification) tuples
            classification = []

            if dictionary_meta["key"] is None: # just a term -- assume no classification/hierarchy
                if dictionary_meta["csv"]:
                    if len(term) > 1:
                        classification = [i for i in term[1:] if i != '']
                    term = term[0]
                to_cache = term.strip()

            # Otherwise follow the given path
            else:
                if dictionary_meta["classification_path"] is not None:
                    for rank in dictionary_meta["classification_path"]:
                        hierarchy_term = term
                        rank = rank.split('.')
                        for field in rank:
                            if field in hierarchy_term:
                                hierarchy_term = hierarchy_term[field]
                            else:
                                hierarchy_term = None
                                continue
                        if hierarchy_term is not None:
                            classification.append(hierarchy_term)
		term = term[dictionary_meta["key"]]

            incoming.add((term, tuple(classification)))

        # remove terms that are no longer around
        for term, classification in list(existing - incoming):
            self.delete_term(dictionary_meta["dict_id"], term, list(classification))
            es_helper.remove_percolator(dictionary_meta["dict_id"], term)

        for term, classification in incoming:
	    print "Working on %s" % term
            classification = list(classification)

            field = self.get_contents_field(dict_name)

            # check if the term exists as-is in the table, before inserting/matching
            ## note: if a term has multiple classifications, it'll still be inserted multiple times
            print self.pg_cursor.execute("""
              SELECT last_updated
              FROM dict_terms
              WHERE dict_id = %(dict_id)s
                AND term = %(term)s
                AND classification = %(classification)s;
            """, {
              "dict_id": dictionary_meta["dict_id"],
              "term" : term,
              "classification" : classification
            })
            self.pg_cursor.execute("""
              SELECT last_updated
              FROM dict_terms
              WHERE dict_id = %(dict_id)s
                AND term = %(term)s
                AND classification = %(classification)s;
            """, {
              "dict_id": dictionary_meta["dict_id"],
              "term" : term,
              "classification" : classification
            })
            date_from = self.pg_cursor.fetchone()
            if date_from is None:
                # Insert into `dict_terms`
                self.insert_into_dict_terms(dictionary_meta["dict_id"], term, classification)
		# Add percolator
		es_helper.add_percolator(dictionary_meta["dict_id"], term, field, classification)
	    else:
		date_from = date_from["last_updated"]


            # Update `terms_docs` self.match_term_to_docs(term, dictionary_meta["case_sensitive"], classification)
#            self.match_term_to_docs(term, field, dictionary_meta["dict_id"], classification, date_from)

            # Update the last updated time in dict_terms
#            self.pg_cursor.execute("""
#              UPDATE dict_terms SET last_updated = now() WHERE dict_id = %(dict_id)s AND term = %(term)s
#            """, {
#              "dict_id": dictionary_meta["dict_id"],
#              "term" : term
#            })
#            self.pg_connection.commit()

        # Update the last updated time for this dictionary
        self.pg_cursor.execute("""
          UPDATE dictionaries SET last_updated = now() WHERE dict_id = %(dict_id)s
        """, {
          "dict_id": dictionary_meta["dict_id"]
        })
        self.pg_connection.commit()

        print "Done updating ", dict_name

    def ingest(self, dict_name, update = False):
        """
        Create/update the terms for a given dictionary source.

        Args:
            dict_name (string): the name of a dictionary to update
            update (bool): If True, remove the term and re-add it, to ensure
            matches against the most recent documents.
        Effects:
            Updates `dict_terms`, Elasticsearch percolators, and matches new terms to documents
        """
        es_helper = ElasticsearchHelper.ElasticsearchHelper(self.config.get("es_index", self.corpus))

        # Fetch metadata about this dictionary
        dictionary_meta = self.get_dictionary_metadata(dict_name)

        if dictionary_meta is None:
            print "Dictionary ", dict_name, " not found"
            sys.exit(1)

        # Check if source is remote or local and load terms
        if "http" in dictionary_meta["source"]:
            source = requests.get(dictionary_meta["source"], verify=False)
            try:
                terms = source.json()
            except ValueError: # API is defined remotely, but isn't json -- likely a CSV file
                terms = []
                for line in source.text.split("\n"):
                    if line == "": continue
                    line = line.split(",")
		    terms.append([i.strip() for i in line])
                    dictionary_meta["csv"] = True
        else:
            with open(dictionary_meta["source"]) as source:
                if dictionary_meta["key"] is None: # either CSV or a one-term-per-line file
                    terms= []
                    for line in source:
                        if line == "": continue
                        line = line.split(",")
                        terms.append([i.strip() for i in line])
                        dictionary_meta["csv"] = True
                else:
                    terms = json.loads(source.read())

        # Slide down the JSON to get the list of objects
        if dictionary_meta["json_path"]:
          for key in dictionary_meta["json_path"]:
              terms = terms[key]

        # Get existing terms
        self.pg_cursor.execute("""
          SELECT term
          FROM dict_terms
          WHERE dict_id = %(dict_id)s
        """, {
          "dict_id": dictionary_meta["dict_id"]
        })

        # IAR - 14.Apr.2017 -- make sure the encoding is the same on the term comparisions
        ## we were comparing unicode to ascii encoding and re-adding terms we already have.
        existing = set( [ unicode(each["term"].strip(), 'utf-8') for each in self.pg_cursor.fetchall() ] )
        if "csv" in dictionary_meta and dictionary_meta["csv"]:
            incoming = set( [ term[0].strip() for term in terms] )
        else:
            incoming = set( [ unicode(term.strip(), 'utf-8') if dictionary_meta["key"] is None else term[dictionary_meta["key"]] for term in terms ] )

        for term in list(existing - incoming):
          # Delete from `dict_terms`
          self.delete_term(dictionary_meta["dict_id"], term)
          # Remove percolator
          es_helper.remove_percolator(dictionary_meta["dict_id"], term)

        # Find terms to insert
        to_insert = list(incoming - existing)
        if update:
            to_insert = list(incoming)
        print to_insert

        # Iterate on the list of terms
        for idx, term in enumerate(terms):
            # Store hierarchy information about the term, if applicable
            classification = []

            # If list of terms, just grab the term
            if dictionary_meta["key"] is None: # just a term -- assume no classification/hierarchy
                if dictionary_meta["csv"]:
                    if len(term) > 1:
                        classification = [i for i in term[1:] if i != '']
                    term = term[0]

                if update:
                    print "Updating term %s" % term
                    # Delete from `dict_terms`
                    self.delete_term(dictionary_meta["dict_id"], term.strip())
                    # Remove percolator
                    es_helper.remove_percolator(dictionary_meta["dict_id"], term)

                to_cache = term.strip()
                # Log progress
                sys.stdout.write("Working on " + term + "(term " + str(idx) + " of " + str(len(terms)) + ")\n")
                sys.stdout.flush()

            # Otherwise follow the given path
            else:
                if update:
                    print "Updating term %s" % term
                    # Delete from `dict_terms`
                    self.delete_term(dictionary_meta["dict_id"], term[dictionary_meta["key"]].strip())
                    # Remove percolator
                    es_helper.remove_percolator(dictionary_meta["dict_id"], term[dictionary_meta["key"]])

                print "%s should be gone" % term
                # Log progress
		sys.stdout.write(u"Working on ".encode('utf-8') + term[dictionary_meta["key"]].encode("utf-8") + "(term " + str(idx) + " of " + str(len(terms)) + ")\n")
		sys.stdout.flush()

                try:
                    to_cache = term[dictionary_meta["key"]]

                    if dictionary_meta["classification_path"] is not None:
                        for rank in dictionary_meta["classification_path"]:
                            hierarchy_term = term
                            rank = rank.split('.')
                            for field in rank:
                                if field in hierarchy_term:
                                    hierarchy_term = hierarchy_term[field]
                                else:
                                    hierarchy_term = None
                                    continue
                            if hierarchy_term is not None:
                                classification.append(hierarchy_term)

                except ValueError:
                    to_cache = None

            # Make sure a value was returned
            if to_cache is None:
                print "Cannot extract term from list"
                sys.exit(1)


            # If it is a new term, insert into `dict_terms`, create a percolator, and index
            if to_cache in to_insert:
                # check if the term exists as-is in the table, before inserting/matching
                ## note: if a term has multiple classifications, it'll still be inserted multiple times
                self.pg_cursor.execute("""
                  SELECT term
                  FROM dict_terms
                  WHERE dict_id = %(dict_id)s
                    AND term = %(term)s
                    AND classification = %(classification)s;
                """, {
                  "dict_id": dictionary_meta["dict_id"],
                  "term" : to_cache,
                  "classification" : classification
                })
                if self.pg_cursor.fetchone() is not None:
                    print "Term + classification already in table! Skipping!"
                    continue
                else:
                    # Insert into `dict_terms`
                    self.insert_into_dict_terms(dictionary_meta["dict_id"], to_cache, classification)

                field = self.get_contents_field(dict_name)

                # Add percolator
                es_helper.add_percolator(dictionary_meta["dict_id"], to_cache, field, classification)

                # Update `terms_docs` self.match_term_to_docs(to_cache, dictionary_meta["case_sensitive"], classification)
#                self.match_term_to_docs(to_cache, field, dictionary_meta["dict_id"], classification)

        # Update the last updated time for this dictionary
        self.pg_cursor.execute("""
          UPDATE dictionaries SET last_updated = now() WHERE dict_id = %(dict_id)s
        """, {
          "dict_id": dictionary_meta["dict_id"]
        })
        self.pg_connection.commit()

        print "Done ingesting ", dict_name
