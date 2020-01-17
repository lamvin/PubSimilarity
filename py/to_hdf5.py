
# coding: utf-8



import zipfile
from tables import *
import time
import csv
import re
import string
import tqdm
import sys
csv.field_size_limit(sys.maxsize)

# In[3]:


class PubExp(IsDescription):
    Id_Art = UInt32Col()
    Ordre = UInt8Col()
    Abstract = StringCol(itemsize=4000, dflt=" ", pos=0)  # character String


# In[4]:


#data_path = "/mnt/disks/sec/data/"
data_path = "home/User1/data/"


# In[5]:


start_time = time.time()
with open(data_path+'pub_full.txt') as f:
    for nb_l, l in enumerate(f):
        pass
print("--- %s seconds ---" % (time.time() - start_time))    


# In[6]:



h5file = open_file(data_path + 'pub_full.h5', mode="w", title="pub_sample")
group = h5file.create_group("/", 'pub_exp')
table = h5file.create_table(group, 'sample', PubExp)
abstract = table.row


# In[ ]:


from gensim.models import TfidfModel
from gensim.corpora import Dictionary
from nltk import PorterStemmer
stemmer = PorterStemmer()
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
sw = set(stopwords.words('english'))


# In[ ]:


def preprocess(x):
    x = x.lower()
    x = re.sub(r'\d+', '', x)
    x = x.translate(str.maketrans('','',string.punctuation)).strip()
    tokens = x.split(' ')
    result = [stemmer.stem(i) for i in tokens if not i in sw]
    return ' '.join(result)
    


# In[ ]:


start_time = time.time()

block_len = 1000
prev_ID = None
block = []
first_block = True
with open(data_path+'pub_full.txt', newline='') as f:
    reader = csv.reader(f,delimiter='\t', quoting=csv.QUOTE_NONE)
    next(reader)
    text = ''
    for i in tqdm(range(nb_l)):
        try:
            line = next(reader)
            abstract['Id_Art'] = int(line[0])
            abstract['Ordre'] = int(line[1])
            cur_text = preprocess(line[2])
            abstract['Abstract'] = cur_text
            abstract.append()
            ID = line[0]
            if ID == prev_ID:
                text = text + ' ' + cur_text
            else:
                block.append(text)
                text = cur_text
            prev_ID = ID
        except(csv.error):
            pass
        if (i+1)%block_len == 0:
            if first_block:
                dct = Dictionary([[line for line in doc.split()] for doc in block])
                first_block = False
            else:
                dct.add_documents([[line for line in doc.split()] for doc in block])
            block = []
dct.add_documents([[line for line in doc.split()] for doc in block])
table.flush()
end_time = time.time()
with open(data_path+'dct.p','wb') as f:
    dct.save(f)
print("--- %s seconds ---" % ( - start_time))  


# In[21]:


h5file.close()

