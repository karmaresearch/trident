
# coding: utf-8

# ## Load the embeddings

# In[199]:

import numpy as np
import collections
import array
import time

SubgraphMeta = collections.namedtuple('SubgraphMeta', 'typ rel ent siz')
Subgraph = collections.namedtuple('Subgraph', 'emb meta')
Result = collections.namedtuple('Result', 'query posh post')
   
inputfile = '/var/scratch/uji300/kgemb/fb15k_avgsubgraphs.bin'
logfile = '/var/scratch/uji300/kgemb/fb15K_subgraphs_valid.txt'
emb_meta_e = '/var/scratch/uji300/kgemb/models/fb15k/best-model/E-meta'
emb_e_path = '/var/scratch/uji300/kgemb/models/fb15k/best-model/E.0'
emb_meta_r = '/var/scratch/uji300/kgemb/models/fb15k/best-model/R-meta'
emb_r_path = '/var/scratch/uji300/kgemb/models/fb15k/best-model/R.0'


# I load the file with the subgraph embeddings produced from Trident (for now I don't need them)

# In[200]:

subgraphs_meta = []
embeddings = []
with open(inputfile, 'rb') as fin:
    b_nsubgraphs = fin.read(8)
    nsubgraphs = int.from_bytes(b_nsubgraphs, byteorder='little', signed=False)
    for i in range(nsubgraphs):
        line = fin.read(25)
        typ = line[0]
        rel = int.from_bytes(line[1:9], byteorder='little', signed=False)
        ent = int.from_bytes(line[9:17], byteorder='little', signed=False)
        siz = int.from_bytes(line[17:], byteorder='little', signed=False)        
        sg = SubgraphMeta(typ=typ, ent=ent, siz=siz, rel=rel)
        subgraphs_meta.append(sg)
    # Load the average embeddings
    emb_meta = fin.read(10)
    dims = int.from_bytes(emb_meta[:2], byteorder='big', signed=False)
    mincard = int.from_bytes(emb_meta[2:], byteorder='big', signed=False)
    for i in range(nsubgraphs):
        b_emb = fin.read(dims * 8)
        emb = np.frombuffer(b_emb, dtype=np.float64)
        embeddings.append(emb)

subgraphs = []
for i in range(nsubgraphs):
    subgraphs.append(Subgraph(emb=embeddings[i], meta=subgraphs_meta[i]))
subgraphs_meta = []
embeddings = []


# Load all the embeddings.

# In[201]:

def load_embeddings(meta, e_path):
    batch_size = 0
    dim = 0
    n = 0
    with open(meta, 'rb') as fmeta:
        raw = fmeta.read(10)
        batch_size = int.from_bytes(raw[:4], byteorder='little', signed=False)
        n = int.from_bytes(raw[4:8], byteorder='little', signed=False)
        dim = int.from_bytes(raw[8:], byteorder='little', signed=False)
    e = np.zeros(shape=(n,dim))
    with open(e_path, 'rb') as fin:
        for i in range(n):
            line = fin.read(8 + dim * 8)            
            emb = np.frombuffer(line[8:], dtype=np.float64)
            e[i] = emb
    return e
emb_e = load_embeddings(emb_meta_e, emb_e_path)
emb_r = load_embeddings(emb_meta_r, emb_r_path)


# now load all the results

# In[231]:

def get_train_valid_data(logfile, graph_type='POS'):
    results = []
    with open(logfile, 'rt') as fin:
        header = fin.readline()
        for l in fin:
            tkns = l.split('\t')
            query = tkns[0]
            pos_answer_subgraph_head = int(tkns[1])
            pos_answer_subgraph_tail = int(tkns[2])
            results.append(Result(query, pos_answer_subgraph_head, pos_answer_subgraph_tail))
    # Create the training data
    data_pos = np.zeros((len(results), 3), dtype=np.int)
    data_spo = np.zeros((len(results), 3), dtype=np.int)
    raw_data_pos = np.zeros((len(results), 3), dtype=np.int)
    raw_data_spo = np.zeros((len(results), 3), dtype=np.int)
    
    for i in range(len(results)):
        query = results[i].query
        tkns = query.split(' ')
        h = int(tkns[0])
        r = int(tkns[1])
        t = int(tkns[2])
        
        
        data_pos[i][0] = r
        data_pos[i][1] = t
        raw_data_pos[i][0] = r
        raw_data_pos[i][1] = t
        raw_data_pos[i][2] = results[i].posh + 1
        
        data_spo[i][0] = r
        data_spo[i][1] = h
        raw_data_spo[i][0] = r
        raw_data_spo[i][1] = h
        raw_data_spo[i][2] = results[i].post + 1
        
        posh = results[i].posh
        post = results[i].post
        
        if posh > 0:
            if posh < 3:
                data_pos[i][2] = 1
            elif  posh < 5:
                data_pos[i][2] = 2
            elif posh < 10:
                data_pos[i][2] = 3
            else:
                data_pos[i][2] = 4
        
        if post > 0:
            if post < 3:
                data_spo[i][2] = 1
            elif  post < 5:
                data_spo[i][2] = 2
            elif post < 10:
                data_spo[i][2] = 3
            else:
                data_spo[i][2] = 4
     
    # Take away 10% which should be used for the validation
    idx_val=np.random.choice(data_pos.shape[0], int(data_pos.shape[0]*0.10), replace=False)

    valid_data_pos = data_pos[idx_val,:]
    train_data_pos = np.delete(data_pos, idx_val, axis=0)
    valid_raw_data_pos = raw_data_pos[idx_val, :]
    train_raw_data_pos = np.delete(raw_data_pos, idx_val, axis=0)
    
    valid_data_spo = data_spo[idx_val,:]
    train_data_spo = np.delete(data_spo, idx_val, axis=0)
    valid_raw_data_spo = raw_data_spo[idx_val, :]
    train_raw_data_spo = np.delete(raw_data_spo, idx_val, axis=0)
    
    classes = np.zeros(5)
    for t in train_data_pos:
        pos = t[2]
        classes[pos] += 1
    print(classes)

    return train_data_pos, valid_data_pos, train_data_spo, valid_data_spo, train_raw_data_pos, valid_raw_data_pos, train_raw_data_spo, valid_raw_data_spo


# In[232]:

train_data_pos, valid_data_pos, train_data_spo, valid_data_spo, train_raw_data_pos, valid_raw_data_pos, train_raw_data_spo, valid_raw_data_spo = get_train_valid_data(logfile)#graph_type='SPO'

train_data = train_data_pos
valid_data = valid_data_pos

#train_data = train_data_spo
#valid_data = valid_data_spo

testCount = 0
for x,y in zip(valid_data_pos, valid_data_spo):
    testCount += 1
    if x[0] != y[0]:
        print("FATAL")
print ("Test count = ", testCount)


# In[235]:

# For all training samples, find the average of top K for each relation
# triple contains the array of (r, e, k)
# where
# r is a relation
# e is an entity
# k is the topk value
def find_average_topk_for_relations(triples):
    relstats = {}
    for sample in triples:
        r = sample[0]
        e = sample[1]
        k = sample[2] 

        if r not in relstats.keys():
            if k == 0: # Means not found
                relstats[r] = (1, 0, k)
            else:
                relstats[r] = (1, 1, k)
        else:
            n, hits, sumK = relstats[r]
            if k == 0:
                relstats[r] = (n+1, hits, sumK + k)
            else:
                relstats[r] = (n+1, hits+1, sumK + k)

    for key,value in relstats.items():
        if value[1] == 0: # No hits
            relstats[key] = (value[0], value[1], value[2], -1)
        else:
            relstats[key] = (value[0], value[1], value[2], int(round(value[2]/value[1])))
        
    return relstats


# In[238]:

relstats_spo = find_average_topk_for_relations(train_raw_data_spo)
relstats_pos = find_average_topk_for_relations(train_raw_data_pos)

output = open("fb15k-per-relation-K.log", 'w')

for key in relstats_pos.keys():
    row = str(key) + " " + str(relstats_pos[key][3]) + " "
    if key in relstats_spo.keys():
        row += str(relstats_spo[key][3])
    else:
        row += "-1"
    row += "\n"
    output.write(row)

for key in relstats_spo.keys():
    if key not in relstats_pos.keys():
        row = str(key) + " " + "-1" + " " + str(relstats_spo[key][3]) + "\n"
        output.write(row)
        
output.close()


# ### Learning

# Learning a simple logistic regression model using tensorflow

# In[182]:

import tensorflow as tf


# #### Params

# In[183]:

learning_rate = 0.01
training_epochs = 50#100
batch_size = 100
display_step = 1
n_input = dims * 2
n_classes = 5
n_hidden_1 = 256 # n neurons first layer
n_hidden_2 = 256 # n neurons second layer


# #### Input

# In[184]:

# Set up the input queues
t_emb_e = tf.constant(emb_e)
t_emb_r = tf.constant(emb_r)

# Initialize the training data
t = tf.constant(train_data)
ds = tf.data.Dataset.from_tensor_slices(t)
ds = ds.shuffle(buffer_size=100)
ds = ds.batch(batch_size)

# Initialize the valid data
t_valid = tf.constant(valid_data)
ds_valid = tf.data.Dataset.from_tensor_slices(t_valid)
ds_valid = ds_valid.batch(batch_size)


#with tf.Session() as default_session:
#    test = default_session.run([t_valid])
    #print(test[0][55])
#    print(len(test[0]))

#iter = ds.make_initializable_iterator()
iter = tf.data.Iterator.from_structure(ds.output_types, ds.output_shapes)
el = iter.get_next()

# Lookup embeddings for the inputs
rel, ent, y = tf.split(el, num_or_size_splits=3, axis=1)
rel = tf.reshape(rel, shape=[-1]) # Flatten the tensor
ent = tf.reshape(ent, shape=[-1]) # Flatten the tensor
rel_emb = tf.nn.embedding_lookup(t_emb_r, rel)
ent_emb = tf.nn.embedding_lookup(t_emb_e, ent)
inp = tf.concat([rel_emb, ent_emb], axis=1)

#Process the labels
y = tf.reshape(y, shape=[-1])
y_hot = tf.one_hot(y, n_classes)


# #### Model

# In[185]:

weights = {
    'h1': tf.Variable(tf.random_normal([n_input, n_hidden_1], dtype=tf.float64), dtype=tf.float64),
    'h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2], dtype=tf.float64), dtype=tf.float64),
    'out': tf.Variable(tf.random_normal([n_hidden_2, n_classes], dtype=tf.float64), dtype=tf.float64)
}
biases = {
    'b1': tf.Variable(tf.random_normal([n_hidden_1], dtype=tf.float64), dtype=tf.float64),
    'b2': tf.Variable(tf.random_normal([n_hidden_2], dtype=tf.float64), dtype=tf.float64),
    'out': tf.Variable(tf.random_normal([n_classes], dtype=tf.float64), dtype=tf.float64)
}
layer_1 = tf.add(tf.matmul(inp, weights['h1']), biases['b1'])
layer_2 = tf.add(tf.matmul(layer_1, weights['h2']), biases['b2'])
out_layer = tf.matmul(layer_2, weights['out']) + biases['out']
pred = tf.nn.softmax_cross_entropy_with_logits_v2(logits=out_layer, labels=y_hot)


# #### Gradient

# In[186]:

loss_op = tf.reduce_mean(pred)
optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
train_op = optimizer.minimize(loss_op)


# #### Testing

# In[187]:

# New operator to perform the predictions
predictions = tf.nn.softmax(out_layer, name='predictions')
# Compare the softmax with the actual values
pred_indices = tf.argmax(predictions, axis=1)
res = tf.equal(pred_indices, y)
res = tf.cast(res, tf.int32)
sres = tf.reduce_sum(res)


# #### Learning

# In[188]:

init = tf.global_variables_initializer()
train_input_initializer = iter.make_initializer(ds)
valid_input_initializer = iter.make_initializer(ds_valid)

with tf.Session() as sess:
    sess.run([init])
    for epoch in range(training_epochs):
        start = time.time()
        sess.run(train_input_initializer)
        # Loop over all batches
        avg_loss = 0
        num_batch = 0
        while True:
            try:
                loss = sess.run([loss_op, train_op])                
                num_batch += 1
                avg_loss += loss[0]
            except (tf.errors.OutOfRangeError, StopIteration):
                break
            except e:
                print(e)
                break
        print('Train epoch', epoch, " Loss={:.6f}".format(avg_loss / num_batch), "Time={:.4f}sec".format(time.time() - start))
        if epoch % 10 == 0:
            # Test the performance on the valid dataset
            sess.run(valid_input_initializer)
            
            num_batch = 0
            correct = 0
            myCorrect = 0
            predLog = open('fb15k-predictions.pos.log', 'w')
            while True:
                try:
                    p = sess.run([sres, pred_indices, y, rel, ent, t_valid])                
                    correct += p[0]
                    
                    #print("# batches = ", num_batch)
                    #print("p = ", len(p))
                    #out = pred_indices.eval()
                    #print("predicted indices: ", p[1])
                    #expected = y.eval()
                    #print("Expected indices : ", p[2])
                    #print("correct predictions = ", p[0])
                    #print('*'*80)
                    row = ""
                    count = 0
                    for r,e,k in zip(p[3], p[4], p[1]):
                        row += str(r) + " " + str(e) + " " + str(k) + "\n"
                        #print(str(r) + " " + str(e) + " " + str(k))
                        #print(p[5][100*num_batch + count])
                        if int(k) == p[5][100*num_batch + count][2]:
                            myCorrect += 1
                        count +=1
                    predLog.write(row)
                    
                    num_batch += 1
                    #print(count, " rows written")
                    
                except (tf.errors.OutOfRangeError, StopIteration):
                    break
                except e:
                    print(e)
            print("Correct predictions= ", correct)
            print("*** MY correct Pred= ", myCorrect)
    predLog.close()
print("Optimization Finished!")


# #### Testing

# In[189]:

get_ipython().system('pwd')


# ## SPO

# In[190]:

# Set up the input queues
t_emb_e = tf.constant(emb_e)
t_emb_r = tf.constant(emb_r)

# Initialize the training data
t = tf.constant(train_data_spo)
ds = tf.data.Dataset.from_tensor_slices(t)
ds = ds.shuffle(buffer_size=100)
ds = ds.batch(batch_size)

# Initialize the valid data
t_valid = tf.constant(valid_data_spo)
ds_valid = tf.data.Dataset.from_tensor_slices(t_valid)
ds_valid = ds_valid.batch(batch_size)


#with tf.Session() as default_session:
#    test = default_session.run([t_valid])
    #print(test[0][55])
#    print(len(test[0]))

#iter = ds.make_initializable_iterator()
iter = tf.data.Iterator.from_structure(ds.output_types, ds.output_shapes)
el = iter.get_next()

# Lookup embeddings for the inputs
rel, ent, y = tf.split(el, num_or_size_splits=3, axis=1)
rel = tf.reshape(rel, shape=[-1]) # Flatten the tensor
ent = tf.reshape(ent, shape=[-1]) # Flatten the tensor
rel_emb = tf.nn.embedding_lookup(t_emb_r, rel)
ent_emb = tf.nn.embedding_lookup(t_emb_e, ent)
inp = tf.concat([rel_emb, ent_emb], axis=1)

#Process the labels
y = tf.reshape(y, shape=[-1])
y_hot = tf.one_hot(y, n_classes)


# In[ ]:




# In[191]:

weights = {
    'h1': tf.Variable(tf.random_normal([n_input, n_hidden_1], dtype=tf.float64), dtype=tf.float64),
    'h2': tf.Variable(tf.random_normal([n_hidden_1, n_hidden_2], dtype=tf.float64), dtype=tf.float64),
    'out': tf.Variable(tf.random_normal([n_hidden_2, n_classes], dtype=tf.float64), dtype=tf.float64)
}
biases = {
    'b1': tf.Variable(tf.random_normal([n_hidden_1], dtype=tf.float64), dtype=tf.float64),
    'b2': tf.Variable(tf.random_normal([n_hidden_2], dtype=tf.float64), dtype=tf.float64),
    'out': tf.Variable(tf.random_normal([n_classes], dtype=tf.float64), dtype=tf.float64)
}
layer_1 = tf.add(tf.matmul(inp, weights['h1']), biases['b1'])
layer_2 = tf.add(tf.matmul(layer_1, weights['h2']), biases['b2'])
out_layer = tf.matmul(layer_2, weights['out']) + biases['out']
pred = tf.nn.softmax_cross_entropy_with_logits_v2(logits=out_layer, labels=y_hot)


# #### Gradient

# In[192]:

loss_op = tf.reduce_mean(pred)
optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
train_op = optimizer.minimize(loss_op)


# #### Testing

# In[193]:

# New operator to perform the predictions
predictions = tf.nn.softmax(out_layer, name='predictions')
# Compare the softmax with the actual values
pred_indices = tf.argmax(predictions, axis=1)
res = tf.equal(pred_indices, y)
res = tf.cast(res, tf.int32)
sres = tf.reduce_sum(res)


# #### Learning

# In[194]:

init = tf.global_variables_initializer()
train_input_initializer = iter.make_initializer(ds)
valid_input_initializer = iter.make_initializer(ds_valid)

with tf.Session() as sess:
    sess.run([init])
    for epoch in range(training_epochs):
        start = time.time()
        sess.run(train_input_initializer)
        # Loop over all batches
        avg_loss = 0
        num_batch = 0
        while True:
            try:
                loss = sess.run([loss_op, train_op])                
                num_batch += 1
                avg_loss += loss[0]
            except (tf.errors.OutOfRangeError, StopIteration):
                break
            except e:
                print(e)
                break
        print('Train epoch', epoch, " Loss={:.6f}".format(avg_loss / num_batch), "Time={:.4f}sec".format(time.time() - start))
        if epoch % 10 == 0:
            # Test the performance on the valid dataset
            sess.run(valid_input_initializer)
            
            num_batch = 0
            correct = 0
            myCorrect = 0
            predLog = open('fb15k-predictions.spo.log', 'w')
            while True:
                try:
                    p = sess.run([sres, pred_indices, y, rel, ent, t_valid])                
                    correct += p[0]
                    
                    #print("# batches = ", num_batch)
                    #print("p = ", len(p))
                    #out = pred_indices.eval()
                    #print("predicted indices: ", p[1])
                    #expected = y.eval()
                    #print("Expected indices : ", p[2])
                    #print("correct predictions = ", p[0])
                    #print('*'*80)
                    row = ""
                    count = 0
                    for r,e,k in zip(p[3], p[4], p[1]):
                        row += str(r) + " " + str(e) + " " + str(k) + "\n"
                        #print(str(r) + " " + str(e) + " " + str(k))
                        #print(p[5][100*num_batch + count])
                        if int(k) == p[5][100*num_batch + count][2]:
                            myCorrect += 1
                        count +=1
                    predLog.write(row)
                    
                    num_batch += 1
                    #print(count, " rows written")
                    
                except (tf.errors.OutOfRangeError, StopIteration):
                    break
                except e:
                    print(e)
            print("Correct predictions= ", correct)
            print("*** MY correct Pred= ", myCorrect)
    predLog.close()
print("Optimization Finished!")


# In[195]:

lines = open('fb15k-predictions.pos.log', 'r').readlines()
output = open("sorted_pos.txt", 'w')

for line in sorted(lines, key=lambda line: line.split()[0]):
    output.write(line)

output.close()

lines = open('fb15k-predictions.spo.log', 'r').readlines()
output = open("sorted_spo.txt", 'w')

for line in sorted(lines, key=lambda line: line.split()[0]):
    output.write(line)

output.close()


# In[198]:

with open('sorted_pos.txt', 'r') as f1:
    lines_pos = f1.readlines()

with open('sorted_spo.txt', 'r') as f2:
    lines_spo = f2.readlines()

output = open("fb15k-dynamicK.log", 'w')
for line_pos, line_spo in zip(lines_pos, lines_spo):
    tokens_pos = line_pos.split()
    tokens_spo = line_spo.split()
    rel_pos = tokens_pos[0]
    ent_pos = tokens_pos[1]
    k_pos   = tokens_pos[2]
    
    rel_spo = tokens_spo[0]
    ent_spo = tokens_spo[1]
    k_spo   = tokens_spo[2]
    
    if rel_pos != rel_spo:
        print("FATAL !!!!!!!!!!!")

    log_line = str(ent_spo) + " " + str(rel_spo) + " " + str(ent_pos) + " " + str(k_pos) + " " + str(k_spo) + "\n"
    output.write(log_line)

output.close()


# In[ ]:



