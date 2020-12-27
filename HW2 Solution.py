#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[ ]:





# In[ ]:


################################## Part 1: Transactions analysis ######################################################


# In[ ]:





# In[ ]:


# 1 . total no of transactions


# In[2]:


file_path = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/bh.csv'


# In[3]:


data = pd.read_csv(file_path, sep='\t', header=None)


# In[4]:


data.columns = ['blockID','hash', 'block_timestamp','n_txs']


# In[5]:


data['n_txs'].sum()


# In[ ]:





# In[6]:


file_path_add = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/addresses.csv'


# In[7]:


data_add = pd.read_csv(file_path_add, sep='\t', header=None)


# In[8]:


data_add.columns = ['addrID','address']


# In[9]:


data_add['address'].count()


# In[ ]:





# In[ ]:


# 2. Address with maximum number of bitcoins and Maximum number of bitcoins


# In[10]:


filepath_out = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/txout.csv'


# In[11]:


filepath_in = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/txin.csv'


# In[12]:


data_in = pd.read_csv(filepath_in, sep='\t', header=None)


# In[13]:


data_out = pd.read_csv(filepath_out, sep='\t', header=None)


# In[14]:


data_in.columns = ['txID', 'input_seq', 'prev_txID', 'prev_output_seq', 'addrID', 'sum']


# In[15]:


data_out.columns = ['txID', 'output_seq', 'addrID', 'sum']


# In[16]:


sum_in = pd.DataFrame(data_in.groupby(['addrID'])['sum'].sum())


# In[17]:


sum_out = pd.DataFrame(data_out.groupby(['addrID'])['sum'].sum())


# In[18]:


result = pd.merge(sum_in, sum_out,how='right',on = 'addrID')


# In[19]:


result=result.fillna(0)


# In[20]:


result['balance'] = result['sum_y'] - result['sum_x']


# In[21]:


print(result['balance'].max())


# In[22]:


result.loc[result['balance'].idxmax()]


# In[ ]:





# In[ ]:


# 3. Avergae balance per address


# In[23]:


print(result['balance'].sum()/8385061)


# In[ ]:





# In[ ]:





# In[ ]:


#4 Average number of input and output transactions per address


# In[24]:


file_path_out = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/txout.csv'


# In[25]:


data_out = pd.read_csv(file_path_out, sep='\t', header=None)


# In[26]:


data_out.columns=['txID','output_seq','addrID','sum']


# In[23]:


print(data_out)


# In[27]:


df = pd.DataFrame(data_out.drop_duplicates('addrID'))


# In[55]:


no_trans_out = data_out['txID'].count()


# In[29]:


out_addr_count= df['addrID'].count()
print(out_addr_count)


# In[30]:


no_trans_out / out_addr_count


# In[31]:


no_trans_out = data_out['txID'].count()


# In[35]:


print(no_trans_out)


# In[ ]:


average_trVal=out_sum/no_trans_out


# In[ ]:





# In[38]:


file_path_in = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/txin.csv'


# In[39]:


data_in = pd.read_csv(file_path_in, sep='\t', header=None)


# In[40]:


data_in.columns = ['txID','input_seq','prev_txID','prev_output_seq','addrID', 'sum']


# In[79]:


df_transactions_count + no_trans_out


# In[59]:


df_transactions_count = data_in['txID'].count()


# In[43]:


ad_in = pd.DataFrame(data_in.drop_duplicates('addrID'))


# In[70]:


addr_count = ad_in['addrID'].count()
print(addr_count)


# In[61]:


df_transactions_count / addr_count


# In[74]:


pd.concat([data_out, data_in])


# In[75]:


con_tab = pd.concat([data_out, data_in])


# In[76]:


totAd = pd.DataFrame(con_tab.drop_duplicates('addrID'))


# In[78]:


tot_no_of_address = totAd['addrID'].count()


# In[80]:


Avg_transPerAddress =  (df_transactions_count + no_trans_out) / tot_no_of_address


# In[81]:


print(Avg_transPerAddress)


# In[ ]:





# In[ ]:


# 5. Transaction with greatest number of inputs


# In[36]:


file_path = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/tx.csv'


# In[37]:


data = pd.read_csv(file_path, sep='\t', header=None)


# In[38]:


data.columns = ['txID','blockID','n_inputs','n_outputs']


# In[39]:


data.loc[data['n_inputs'].idxmax()]


# In[ ]:





# In[ ]:


# 6. average value of tranasaction


# In[32]:


df = pd.DataFrame(data_out.drop_duplicates('txID'))
no_trans_out = df['txID'].count()
out_sum= data_out['sum'].sum()
average_trVal=out_sum/no_trans_out
print(average_trVal)


# In[ ]:





# In[ ]:


# 7. Number of Coinbase Transactions


# In[30]:


data.loc[data['n_inputs'] == 0].count()


# In[ ]:





# In[ ]:


# 8. avg no of transaction


# In[10]:


total_transactions = data['n_txs'].sum()


# In[11]:


number_of_blocks = data['blockID'].count()


# In[12]:


Average_transaction_per_block = total_transactions / number_of_blocks


# In[13]:


print(Average_transaction_per_block)


# In[ ]:





# In[ ]:


################################## Part 2: Address de-anonymization ######################################################


# In[ ]:


# 1.


# In[10]:


create_frozen_inDataSet = data_in.groupby('txID')['addrID'].apply(frozenset)


# In[51]:


dataframe_in= pd.DataFrame(create_frozen_inDataSet)


# In[50]:


dataframe_in.reset_index()


# In[49]:


universal_users = set()


# In[48]:


addrID = dataframe_in['addrID'].iloc[0]
addrIDFS = frozenset(addrID)
users.add(addrIDFS)


# In[47]:


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


# In[40]:


file_path_addr = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/addr_sccs.dat'


# In[41]:


data_add = pd.read_csv(file_path_addr, sep='\t', header=None)


# In[42]:


data_add.columns = ['addrID','userID']


# In[43]:


uniq_users = pd.DataFrame(data_add.drop_duplicates('userID'))


# In[128]:


total_users = data_add['userID'].count()


# In[129]:


total_users


# In[ ]:





# In[ ]:





# In[ ]:


#2 b. Average balance per user 


# In[130]:


balance = result['balance'].sum()


# In[131]:


average_bal_pu= balance / total_users
print(average_bal_pu)


# In[ ]:





# In[ ]:


# 2 c. Average number of input and output transaction per user and average transaction per user. 


# In[132]:


aver_input_per_user = df_transactions_count / total_users
print(aver_input_per_user)


# In[133]:


average_output_per_user = no_trans_out / total_users
print(average_output_per_user)


# In[134]:


total_Transaction_avg = (df_transactions_count + no_trans_out) / total_users
print(total_Transaction_avg)


# In[ ]:





# In[ ]:


# 3. hash of the transaction sending the greatest number of bitcoins to the user who is holding the greatest balance


# In[ ]:





# In[64]:


file_path_tx = 'C:/Users/prasi/OneDrive/Desktop/Vejay/Spring 2020/BlockChain/hw/HW2/DATA/compressed/data/tx.csv'


# In[65]:


data_tx = pd.read_csv(file_path_tx, sep='\t', header=None)


# In[66]:


data_tx.columns = ['txID', 'blockID', 'n_inputs', 'n_outputs']


# In[85]:


commonTrans = pd.DataFrame(data_tx.loc[data_tx['n_outputs'] == 1])


# In[87]:


c1 = commonTrans.reset_index()


# In[98]:


c2 = pd.DataFrame(c1['txID'])
c2['txID']


# In[95]:


data_out


# In[100]:


newtableaddress = pd.DataFrame(data_out.loc[data_out['txID'].isin(c2['txID'])])


# In[118]:


addrTable = pd.DataFrame(newtableaddress['addrID'])


# In[119]:


addrTable


# In[120]:


tableuiqadd =pd.DataFrame(~addrTable.loc[addrTable['addrID'].isin(data_add['addrID'])]) 


# In[121]:


tableuiqadd


# In[ ]:




