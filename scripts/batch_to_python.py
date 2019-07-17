import sys
import trident
import os

infile = sys.argv[1]
dbfile = sys.argv[2]
outdir = sys.argv[3]

query_counter = 0
db = trident.Db(dbfile)
prev_p = None
prev_s = None
out_file = None
with open(infile, "rb") as in_file:
    while True:
        piece = in_file.read(15)
        if piece == b'':
            break # end of file
        subj = int.from_bytes(piece[0:5], byteorder='little', signed=False)
        pred = int.from_bytes(piece[5:10], byteorder='little', signed=False)
        #obj = int.from_bytes(piece[10:15], byteorder='little', signed=False)
        subj_s = db.lookup_str(subj)
        pred_s = db.lookup_relstr(pred)
        #obj_s = db.lookup_str(obj)
        if prev_p is None or prev_p != pred or prev_s is None or prev_s != subj:
            if out_file is not None:
                out_file.close()
            if prev_p is None or prev_p != pred:
                if not os.path.exists(outdir + '/' + str(pred)):
                    os.makedirs(outdir + '/' + str(pred))
            out_file = open(outdir + '/' + str(pred) + '/query-' + str(query_counter) + '.json', "wt")
            query_counter += 1
            out_file.write('{ "subject" : "%s", "predicate" : "%s", "object" : "?" }' % (subj_s, pred_s))
        prev_p = pred
        prev_s = subj
