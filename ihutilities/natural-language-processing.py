#!/usr/bin/env python
# encoding: utf-8

import glob
import pandas
import nltk
import time

from nltk.corpus import stopwords
from nltk.probability import FreqDist
from collections import Counter
from nltk.corpus import wordnet as wn

import time
import csv

frames = []

def load_data_from_raw():
    # Takes 3s 
    DATA_DIR = 'C:\\Users\\Ian\\BigData\\2116-BIGLottery'
    for filename in glob.glob(DATA_DIR + '\\*.csv'):
        print filename
        frames.append(pandas.read_csv(filename))

    allframes = pandas.concat(frames)

    print allframes
    return allframes

t0 = time.time()
# allframes = load_data_from_raw()
allframes = pandas.DataFrame.load('BIGLottery-all-pandas.df')
t1 = time.time()

print allframes
total = t1 - t0

print total

#allframes.save('BIGLottery-all-pandas.df')



# Do word frequency count on press_summary

stopwords = nltk.corpus.stopwords.words('english')
corpus = []
word_counter = Counter()
for entry in allframes['PRESS_SUMMARY']:
    if isinstance(entry, str):
        words = entry.split()
        for word in words:
            if word not in stopwords:
                corpus.append(word.lower())
                word_counter[word.lower()] += 1
    else:
        continue
# We should actually do this by counting words and then doing the classification

sporty_counter = Counter()
sporty_dist = {}
for word in word_counter.keys():
    if word_counter[word] > 10:
        synset = wn.synsets(word)
        if len(synset) > 0:
            for def_ in synset:   
                sporty = wn.synsets("sports")[0].wup_similarity(def_)
                if sporty >= 0.8:
                    sporty_counter[word] = word_counter[word]
                    sporty_dist[word] = sporty
#fd =  FreqDist(corpus)

#Output the data
with open("sports-summary.csv",'wb') as outfile:
    writer = csv.writer(outfile, delimiter=',')
    for key, value in sporty_counter.iteritems():
        writer.writerow([key, value])

print sporty_counter
print sporty_dist

import csv
with open('sporty_dist.csv', 'wb') as csvfile:
   output = csv.writer(csvfile, delimiter=',',
                           quotechar='"', quoting=csv.QUOTE_MINIMAL)

   for key, value in sporty_dist.iteritems():
       output.writerow([key, value])

#import csv
# with open('APPLICANT_NAME.csv', 'wb') as csvfile:
#    output = csv.writer(csvfile, delimiter=',',
#                            quotechar='"', quoting=csv.QUOTE_MINIMAL)

#    for key, value in fd.iteritems():
#        output.writerow([key, value])

# How about pulling out all sport related words using nltk synset
