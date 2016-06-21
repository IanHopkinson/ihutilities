#!/usr/bin/env python

from __future__ import print_function
from __future__ import unicode_literals

import json
import os
import sys
import urllib

import requests

import nltk
from nltk.util import ngrams
from nltk.tokenize import RegexpTokenizer

import scraperwiki

from collections import Counter

# Test url https://ds-ec2.scraperwiki.com/otzbwqj/im7lezk5tvads04/sql/?q=select%20%0A%09nice_name%0Afrom%20swdata%0Alimit%2020%0A

def main(argv=None):
    if argv is None:
        argv = sys.argv
    arg = argv[1:]

    if len(arg) > 0:
        # Developers can supply a text_field as an argument...
        text_url = arg[0]
    else:
        # ... but normally the URL comes from the allSettings.json file
        with open(os.path.expanduser("~/allSettings.json")) as settings:
            text_url = json.load(settings)['source-url']

    # We need to get the box name and public key to 
    return get_ngrams(text_url)

def get_ngrams(text_url):
    """
    Get ngrams for a text_field
    """

    # Get data over the SQL API
    text_data = get_data_by_api(text_url)

    # Do ngram calculations (1-gram, 2-gram, 3-gram)
    ngram_counter = ngram_frequencies(text_data)
    # convert 
    ngram_data = []
    for key in ngram_counter.keys():
        row = {}
        row['ngram'] = '_'.join(key) #.encode('utf-8')
        row['ngram_count'] = ngram_counter[key]
        row['n'] = len(key)
        ngram_data.append(row)
    # Write ngram data to database (ngram, ngram_count, n (i.e. 1, 2, 3))
    scraperwiki.sql.execute("DROP TABLE IF EXISTS ngrams")
    scraperwiki.sql.save([], ngram_data, table_name="ngrams")    

def get_data_by_api(text_url):
    r = requests.get(text_url)
    text_data = r.json()
    return text_data

def ngram_frequencies(text_data, limit=float("inf")):
    tokenizer = RegexpTokenizer(r'\w+')
    stopwords = nltk.corpus.stopwords.words('english')
    ngram_counter = Counter()
    for i, entry in enumerate(text_data):
        if i > limit:
            break
        words = tokenizer.tokenize(entry.values()[0])
        words_lower = [word.lower() for word in words if word not in stopwords]
        
        # 1-grams
        for word in words_lower:
            ngram_counter[(word,)] += 1

        # 2-grams
        bigrams = ngrams(words_lower, 2)
        for bigram in bigrams:
            ngram_counter[bigram] += 1

        # 3-grams
        trigrams = ngrams(words_lower, 3)
        for trigram in trigrams:
            ngram_counter[trigram] += 1


    return ngram_counter

if __name__ == '__main__':
    main()