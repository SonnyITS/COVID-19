#!/usr/bin/env python
# coding: utf-8

# In[276]:


import numpy as np
import pandas as pd
import io
import requests
import re
import datetime


# In[277]:


### Start of importing raw data from github

urlC = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
urlD = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"

sC = requests.get(urlC).content
sD = requests.get(urlD).content

dfC = pd.read_csv(io.StringIO(sC.decode("utf-8")))
dfD = pd.read_csv(io.StringIO(sD.decode("utf-8")))

dfC

### End of importing raw data from github


# In[278]:


### Start of US population dataframe creation

df_USPop = dfD[dfD["Admin2"] != "Unassigned"].copy()
df_USPop_temp = dfD[dfD["Admin2"].isnull()].copy()
df_USPop = df_USPop[df_USPop["Admin2"].str.contains("Out of") == False]
df_USPop = df_USPop.append(df_USPop_temp)
df_USPop = df_USPop[df_USPop["Province_State"].str.contains("Princess") == False]

df_USPop_county = df_USPop[["FIPS", "Admin2", "Province_State", "Combined_Key", "Population"]].copy()
fips = df_USPop_county['FIPS'].to_numpy()
fips = fips.astype(int)
fips = fips.astype(str)
for i in range(fips.size):
    while (len(fips[i]) < 5):
        fips[i] = "0" + fips[i]
        #print(fips[i])
    fips[i] = "\"" + fips[i] + "\""
df_USPop_county["FIPS"] = fips
df_USPop_county = df_USPop_county.set_index('FIPS') 

df_USPop_State = df_USPop[["Province_State", "Population"]].copy()
df_USPop_State = df_USPop_State.groupby("Province_State").sum()

#df_USPop_county
df_USPop_State

### End of US population dataframe creation


# In[279]:


### Start of US states confirmed time series

dfC_US = dfC[dfC["Admin2"] != "Unassigned"].copy()
dfC_US_temp = dfC[dfC["Admin2"].isnull()].copy()
dfC_US = dfC_US[dfC_US["Admin2"].str.contains("Out of") == False]
dfC_US = dfC_US.append(dfC_US_temp)
dfC_US = dfC_US[dfC_US["Province_State"].str.contains("Princess") == False]


dfC_US = dfC_US.drop(columns = ["Country_Region", "Lat", "Long_", "Admin2", "UID", "iso2", "iso3", "code3", "FIPS", "Combined_Key"])
dfC_US = dfC_US.groupby("Province_State").sum()

dfC_US = dfC_US.transpose()
dfC_US.to_csv("US_States_TimeSeries_COVID19_Confirmed.csv", index_label="Date")

dfC_US
#dfC_US_temp

### End of US states confirmed time series


# In[280]:


### Start of US states confirmed per 100k residents time series

dfC_perCap = dfC_US.copy().transpose()
dfC_perCap = pd.merge(dfC_perCap, df_USPop_State, left_index=True, right_index=True, how="inner")

dfC_perCap = dfC_perCap.div(dfC_perCap["Population"], axis=0)
dfC_perCap = dfC_perCap.mul(100000, axis=0)

dfC_perCap = dfC_perCap.drop(columns = ["Population"])
dfC_perCap = dfC_perCap.transpose()
dfC_perCap.to_csv("US_States_TimeSeries_COVID19_ConfirmedPer100k.csv", index_label="Date")

dfC_perCap

### End of US states confirmed per 100k residents time series


# In[281]:


### Start of US states deaths time series

dfD_US = dfD[dfD["Admin2"] != "Unassigned"].copy()
dfD_US_temp = dfD[dfD["Admin2"].isnull()].copy()
dfD_US = dfD_US[dfD_US["Admin2"].str.contains("Out of") == False]
dfD_US = dfD_US.append(dfD_US_temp)
dfD_US = dfD_US[dfD_US["Province_State"].str.contains("Princess") == False]


dfD_US = dfD_US.drop(columns = ["Country_Region", "Lat", "Long_", "Admin2", "UID", "iso2", "iso3", "code3", "FIPS", "Combined_Key", "Population"])
dfD_US = dfD_US.groupby("Province_State").sum()

dfD_US = dfD_US.transpose()
dfD_US.to_csv("US_States_TimeSeries_COVID19_Deaths.csv", index_label="Date")

dfD_US
#dfD_US_temp

### End of US states deaths time series


# In[282]:


### Start of US states deaths per 100k residents time series

dfD_perCap = dfD_US.copy().transpose()
dfD_perCap = pd.merge(dfD_perCap, df_USPop_State, left_index=True, right_index=True, how="inner")

dfD_perCap = dfD_perCap.div(dfD_perCap["Population"], axis=0)
dfD_perCap = dfD_perCap.mul(100000, axis=0)

dfD_perCap = dfD_perCap.drop(columns = ["Population"])
dfD_perCap = dfD_perCap.transpose()
dfD_perCap.to_csv("US_States_TimeSeries_COVID19_DeathsPer100k.csv", index_label="Date")

dfD_perCap

### End of US states deaths per 100k residents time series


# In[283]:


### Start of US county confirmed time series

dfC_US_county = dfC[dfC["Admin2"] != "Unassigned"].copy()
dfC_US_county_temp = dfC[dfC["Admin2"].isnull()].copy()
dfC_US_county = dfC_US_county[dfC_US_county["Admin2"].str.contains("Out of") == False]
dfC_US_county = dfC_US_county.append(dfC_US_county_temp)
dfC_US_county = dfC_US_county[dfC_US_county["Province_State"].str.contains("Princess") == False]

dfC_US_county = dfC_US_county.drop(columns = ["Country_Region", "Lat", "Long_", "UID", "iso2", "iso3", "code3", "Combined_Key", "Admin2", "Province_State"])
dfC_US_county = dfC_US_county.dropna()

fips = dfC_US_county['FIPS'].to_numpy()
fips = fips.astype(int)
fips = fips.astype(str)


for i in range(fips.size):
    while (len(fips[i]) < 5):
        fips[i] = "0" + fips[i]
        #print(fips[i])
    fips[i] = "\"" + fips[i] + "\""

dfC_US_county["FIPS"] = fips
#dfC_US = dfC_US.drop(columns = ["Combined_Key"])
dfC_US_county = dfC_US_county.set_index('FIPS') 
#dfC_US = dfC_US.drop(dfC_US.columns[1:-1], axis = 1)
dfC_US_county = dfC_US_county.transpose()
dfC_US_county.to_csv("US_County_TimeSeries_COVID19_Confirmed.csv", index_label="Date")

dfC_US_county
#dfC_US_county_temp

### End of US county confirmed time series


# In[284]:


### Start of US county deaths time series

dfD_US_county = dfD[dfD["Admin2"] != "Unassigned"].copy()
dfD_US_county_temp = dfD[dfD["Admin2"].isnull()].copy()
dfD_US_county = dfD_US_county[dfD_US_county["Admin2"].str.contains("Out of") == False]
dfD_US_county = dfD_US_county.append(dfD_US_county_temp)
dfD_US_county = dfD_US_county[dfD_US_county["Province_State"].str.contains("Princess") == False]

dfD_US_county = dfD_US_county.drop(columns = ["Country_Region", "Lat", "Long_", "UID", "iso2", "iso3", "code3", "Combined_Key", "Population", "Admin2", "Province_State"])
dfD_US_county = dfD_US_county.dropna()

fips = dfD_US_county['FIPS'].to_numpy()
fips = fips.astype(int)
fips = fips.astype(str)

for i in range(fips.size):
    while (len(fips[i]) < 5):
        fips[i] = "0" + fips[i]
        #print(fips[i])
    fips[i] = "\"" + fips[i] + "\""

dfD_US_county["FIPS"] = fips
#dfD_US = dfD_US.drop(columns = ["Combined_Key"])
dfD_US_county = dfD_US_county.set_index('FIPS') 
#dfD_US = dfD_US.drop(dfD_US.columns[1:-1], axis = 1)
dfD_US_county = dfD_US_county.transpose()
dfD_US_county.to_csv("US_County_TimeSeries_COVID19_Deaths.csv", index_label="Date")

dfD_US_county
#dfD_US_county_temp

### End of US county deaths time series


# In[285]:


### Start of US states cross section of latest data

# Total Confirmed Cases
df1 = dfC_US.copy().transpose()
df1 = df1.drop(df1.columns[0:-1], axis=1)
df1.columns = ['Confirmed']


# Confirmed per 100K
df2 = dfC_perCap.copy().transpose()
df2 = df2.drop(df2.columns[0:-1], axis=1)
df2.columns = ['Confirmed per 100K']


# Total Deaths
df3 = dfD_US.copy().transpose()
df3 = df3.drop(df3.columns[0:-1], axis=1)
df3.columns = ['Deaths']


# Deaths per 100K
df4 = dfD_perCap.copy().transpose()
df4 = df4.drop(df4.columns[0:-1], axis=1)
df4.columns = ['Deaths per 100K']


# Cross section of current state data merging
df_cross_section = pd.merge(df1, df2, left_index=True, right_index=True, how="inner")
df_cross_section = pd.merge(df_cross_section, df3, left_index=True, right_index=True, how="inner")
df_cross_section = pd.merge(df_cross_section, df4, left_index=True, right_index=True, how="inner")

df_cross_section.to_csv("US_States_CrossSection_COVID19_Deaths.csv")

df_cross_section
#df1
#df2
#df3
#df4

### End of US states cross section of latest data


# In[286]:


### Start of US county cross section of latest date

# Total Confirmed Cases
df1 = dfC_US_county.copy().transpose()
df1 = df1.drop(df1.columns[0:-1], axis=1)
df1.columns = ['Confirmed']

df_cross_section = pd.merge(df_USPop_county, df1, left_index=True, right_index=True, how="inner")


# Confirmed Cases per 100k residents
df_cross_section["Confirmed per 100K"] = df_cross_section["Confirmed"]/df_cross_section["Population"]
df_cross_section["Confirmed per 100K"] = df_cross_section["Confirmed per 100K"] * 100000


# Total Deaths
df3 = dfD_US_county.copy().transpose()
df3 = df3.drop(df3.columns[0:-1], axis=1)
df3.columns = ['Deaths']

df_cross_section = pd.merge(df_cross_section, df3, left_index=True, right_index=True, how="inner")


# Confirmed Cases per 100k residents
df_cross_section["Deaths per 100K"] = df_cross_section["Deaths"]/df_cross_section["Population"]
df_cross_section["Deaths per 100K"] = df_cross_section["Deaths per 100K"] * 100000


df_cross_section.to_csv("US_County_CrossSection_COVID19_Deaths.csv")


#df1
#df_USPop_county
df_cross_section


# In[287]:


get_ipython().system('jupyter nbconvert --to script Master US Data Cleaner.ipynb')

