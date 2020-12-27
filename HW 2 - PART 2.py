#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[ ]:





# In[ ]:


################################## Part 2: Address de-anonymization ######################################################


# In[2]:


file_path_in = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/txin.csv'


# In[3]:


data_in = pd.read_csv(file_path_in, sep='\t', header=None)


# In[4]:


data_in.columns = ['txID','input_seq','prev_txID','prev_output_seq','addrID', 'sum']


# In[5]:


file_path_out = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/txout.csv'


# In[6]:


data_out = pd.read_csv(file_path_out, sep='\t', header=None)


# In[8]:


data_out.columns=['txID','output_seq','addrID','sum']


# In[9]:


sum_in = pd.DataFrame(data_in.groupby(['addrID'])['sum'].sum())

sum_out = pd.DataFrame(data_out.groupby(['addrID'])['sum'].sum())

result = pd.merge(sum_in, sum_out,how='right',on = 'addrID')

result=result.fillna(0)

result['balance'] = result['sum_y'] - result['sum_x']


# In[ ]:





# In[11]:


create_frozen_inDataSet = data_in.groupby('txID')['addrID'].apply(frozenset)


# In[ ]:


dataframe_in= pd.DataFrame(create_frozen_inDataSet)


# In[ ]:


universal_users = set()


# In[ ]:


addrID = dataframe_in['addrID'].iloc[0]
addrIDFS = frozenset(addrID)
users.add(addrIDFS)


# In[ ]:


for i in range(len(dataframe_in)): 
    if i > 0:
        addrID = set(tmpdf['addrID'].iloc[i])
        newuserFS = frozenset(addrID)
        for user in users:
            userSet = set(user)
        
            if len(userSet.intersection(addrID)) > 0:
                users.remove(user)
                n_user = userSet.union(addrID)
                n_userFS = frozenset(n_user)
                break
        users.add(n_userFS)


# In[ ]:


file_path_addr = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/addr_sccs.dat'


# In[ ]:


data_add = pd.read_csv(file_path_addr, sep='\t', header=None)


# In[ ]:


data_add.columns = ['addrID','userID']


# In[ ]:


uniq_users = pd.DataFrame(data_add.drop_duplicates('userID'))


# In[ ]:


total_users = data_add['userID'].count()


# In[ ]:


total_users


# In[ ]:





# In[ ]:


#2 b. Average balance per user 


# In[ ]:


balance = result['balance'].sum()


# In[ ]:


average_bal_pu= balance / total_users
print(average_bal_pu)


# In[ ]:





# In[ ]:


# 2 c. Average number of input and output transaction per user and average transaction per user. 


# In[ ]:


aver_input_per_user = df_transactions_count / total_users
print(aver_input_per_user)


# In[ ]:





# In[ ]:


average_output_per_user = no_trans_out / total_users
print(average_output_per_user)


# In[ ]:





# In[ ]:


total_Transaction_avg = (df_transactions_count + no_trans_out) / total_users
print(total_Transaction_avg)


# In[ ]:





# In[ ]:


# 1 for finding serial control


# In[ ]:


file_path_tx = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/tx.csv'


# In[ ]:


data_tx = pd.read_csv(file_path_tx, sep='\t', header=None)

data_tx.columns = ['txID', 'blockID', 'n_inputs', 'n_outputs']

commonTrans = pd.DataFrame(data_tx.loc[data_tx['n_outputs'] == 1])

c1 = commonTrans.reset_index()

c2 = pd.DataFrame(c1['txID'])
c2['txID']


# In[ ]:





# In[ ]:


newtableaddress = pd.DataFrame(data_out.loc[data_out['txID'].isin(c2['txID'])])

addrTable = pd.DataFrame(newtableaddress['addrID'])

addrTable


# In[ ]:





# In[ ]:




