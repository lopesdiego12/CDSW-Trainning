
# coding: utf-8

# In[2]:


import numpy as np

a = np.array([0,1,2,3,4,5])
print(a)


# In[3]:


type(a)


# In[4]:


a.dtype


# In[5]:


a = np.array([[0,1,2,3], [4,5,6,7], [8,9,10,11]])


# In[6]:


x = np.arange(11.)


# In[7]:


x


# In[8]:


x = np.arange(10, 30, 5)


# In[9]:


x


# In[10]:


a.shape


# In[11]:


a.shape = (2,6)


# In[12]:


a


# In[13]:


a[1,3]


# In[14]:


a[0,3:5]


# In[81]:


import numpy as np
import pandas as pd

#data = np.loadtxt('dataset.csv', delimiter=';', skiprows=1)
#data = np.genfromtxt('dataset.csv', delimiter=';', names=True)
df = pd.read_csv('dataset.csv', sep=';', header=0, decimal=',')


# In[6]:


data


# In[11]:


data['Produto']


# In[82]:


df = df.set_index('Codigo')
df.head()


# In[83]:


df.groupby(['Produto']).sum()


# In[84]:


df['Produto'].count()


# In[60]:


df['Quantidade'].max()


# In[85]:


df['Valor_Unitario'][df['Produto'] == 'FANTA'].sum()


# In[86]:


df[df['Produto'] == 'FANTA'].groupby('Produto').sum()


# In[87]:


df.groupby(['Produto']).agg({"Quantidade": 'min'})


# In[64]:


df.groupby(['Produto']).agg({"Quantidade": ['min','max','sum', 'mean']})


# In[88]:


df['Valor_Total'] = df.Quantidade * df.Valor_Unitario


# In[89]:


df.groupby('Produto').sum()


# In[92]:


df.groupby(['Produto', 'Valor_Unitario', 'Quantidade'])['Valor_Total'].sum()

