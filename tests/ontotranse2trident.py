import pickle
import sys
import gzip

dbin = pickle.load(open(sys.argv[1], 'rb'))
fvalid = open(sys.argv[2] + "/valid_data", 'wt')
for t in dbin['valid_subs']:
    fvalid.write(str(t[0]) + ' ' + str(t[2]) + ' ' + str(t[1]) + '\n')
fvalid.close()

triples = gzip.open(sys.argv[2] + '/triples.gz', 'wt')
for t in dbin['train_subs']:
    triples.write(str(t[0]) + ' ' + str(t[2]) + ' ' + str(t[1]) + '\n')
triples.close()
dictfile = gzip.open(sys.argv[2] + '/dict.gz', 'wt')
for k,v in dbin['entities'].items():
    dictfile.write(str(k) + ' ' + str(len(v)) + ' ' + v + '\n')
dictfile.close()
