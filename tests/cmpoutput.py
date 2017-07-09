import os
import sys
import pickle


realset = pickle.load(open(sys.argv[2], 'rb'))
realset = set(realset['train_subs'])

debugdir = sys.argv[1]
files = os.listdir(debugdir)
for e in range(10):
    outputset = set()
    countp = 0
    allsneg = []
    alloneg = []
    for f in files:
        if "batch-" + str(e) + '-' in f:
            fin = open(debugdir + "/" + f, 'rb')
            b = fin.read(8)
            sizebuffer = int.from_bytes(b, 'little')
            for j in range(sizebuffer):
                a = fin.read(8)
                b = fin.read(8)
                c = fin.read(8)
                sneg = fin.read(8)
                oneg = fin.read(8)
                s = int.from_bytes(a, 'little')
                p = int.from_bytes(b, 'little')
                o = int.from_bytes(c, 'little')
                sneg = int.from_bytes(sneg, 'little')
                allsneg.append(sneg)
                oneg = int.from_bytes(oneg, 'little')
                alloneg.append(oneg)
                outputset.add((s,o,p))
                if (sneg, o, p) in realset:
                    countp += 1
    print("The set contains ", len(outputset), countp, outputset == realset)
    for i in range(10):
        print(alloneg[i])
