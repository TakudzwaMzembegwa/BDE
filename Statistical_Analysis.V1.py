#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller
# for covariance
from numpy import cov
# for correlation
from scipy.stats import pearsonr
from scipy.stats import skew
import scipy.stats as stats
import seaborn as sns
sns.set(style="darkgrid")
get_ipython().run_line_magic('matplotlib', 'inline')
plt.style.use('seaborn')
import scipy.stats as st
import warnings
warnings.filterwarnings('ignore')


# In[2]:


data = pd.read_csv('Documents/COS736/ElsiesRiver.csv')


# In[3]:


data.head(5)


# In[4]:


data.info()


# In[5]:


data.isnull().sum()


# In[6]:


data.describe()


# In[7]:


plt.rcParams['figure.figsize'] = (10, 6)
plt.xlabel('Temperature_F')
plt.ylabel('Density')
plt.title('Density Plot')
sns.distplot(data['Temperature_F'])


# In[8]:


import seaborn as sns
from matplotlib import style

sns.distplot(data["Temperature_F"], color="red")
sns.distplot(data["Humidity_%"], color="green")
plt.legend(['Temperature_F', 'Humidity_%'], loc='upper left')
plt.title("Density plot of the Temperature_F and Humidity_%.", color="purple")


# In[9]:


Temperature_F = data[['Temperature_F', 'Humidity_%']]
number_of_columns = len(Temperature_F.columns)
plt.rcParams['figure.figsize'] = (22, 9)
for category in range(0,number_of_columns):
    plt.subplot(2 ,number_of_columns ,category+1)
    sns.boxplot(Temperature_F[Temperature_F.columns[category]], orient='v')


# In[10]:


plt.title('CORRELATION PLOT')
plt.rcParams['figure.figsize'] = (10, 2)
sns.heatmap(data.corr(), annot= True, cmap = 'Blues', fmt='.2g')


# In[11]:


datae = data[['Temperature_F']]
datae.rolling(12).mean().plot(figsize=(20,10), linewidth=5, fontsize=20)
plt.xlabel('created_at', fontsize=20)
plt.title("Temperature_F trend.", color="purple", fontsize=30);


# In[12]:


datae.diff().plot(figsize=(20,10), linewidth=5, fontsize=20)
plt.xlabel('Date', fontsize=20)
plt.title("Seasonal trend.", color="purple", fontsize=30);


# In[13]:


plt.rcParams['figure.figsize'] = (10, 7)
sns.scatterplot(data['created_at'],data['Temperature_F'], color= "Blue" )


# In[14]:


sns.pairplot(data)


# In[18]:


adfuller_result = adfuller(data.Temperature_F.values, autolag='AIC')

print(f'ADF Statistic: {adfuller_result[0]}')

print(f'p-value: {adfuller_result[1]}')

for key, value in adfuller_result[4].items():
    print('Critical Values:')
    print(f' {key}, {value}')


# In[25]:


T = data["Temperature_F"]
P10 = data["PM10.0_CF1"]


# In[26]:


print("Skewness for data :",skew(T))
print("Skewness for data :",skew(P10))


# In[27]:


kurtosis_scipy = stats.kurtosis(T)
kurtosis_scipyi = stats.kurtosis(P10)
print(kurtosis_scipy)
print(kurtosis_scipyi)


# In[ ]:




