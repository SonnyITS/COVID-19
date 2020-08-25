#!/usr/bin/env python
# coding: utf-8

# In[42]:


import numpy as np
import pandas as pd
import io
import requests
import re
import datetime


# In[43]:


### Start of importing raw data from github

urlC = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
urlD = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"

sC = requests.get(urlC).content
sD = requests.get(urlD).content

dfC = pd.read_csv(io.StringIO(sC.decode("utf-8")))
dfD = pd.read_csv(io.StringIO(sD.decode("utf-8")))

dfC

### End of importing raw data from github


# In[44]:


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


# In[45]:


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


# In[46]:


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


# In[47]:


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


# In[48]:


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


# In[49]:


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


# In[50]:


### Start of US county confirmed per 100k residents time series

dfC_countyPerCap = dfC_US_county.copy().transpose()
dfC_countyPerCap_temp = df_USPop_county.copy()
dfC_countyPerCap_temp = dfC_countyPerCap_temp[["Population"]]

dfC_countyPerCap = pd.merge(dfC_countyPerCap, dfC_countyPerCap_temp, left_index=True, right_index=True, how="inner")
dfC_countyPerCap = dfC_countyPerCap.div(dfC_countyPerCap["Population"], axis=0)
dfC_countyPerCap = dfC_countyPerCap.mul(100000, axis=0)

dfC_countyPerCap = dfC_countyPerCap.drop(columns = ["Population"])
dfC_countyPerCap = dfC_countyPerCap.transpose()
dfC_countyPerCap.to_csv("US_County_TimeSeries_COVID19_ConfirmedPer100k.csv", index_label="Date")

# Limiting Data size
dfC_countyPerCap_limited = dfC_countyPerCap.copy().transpose()
dfC_countyPerCap_limited = pd.merge(dfC_countyPerCap_limited, df_USPop_county, left_index=True, right_index=True, how="inner")
selectedStates = ["Connecticut"]
dfC_countyPerCap_limited = dfC_countyPerCap_limited.loc[dfC_countyPerCap_limited["Province_State"].isin(selectedStates)]
dfC_countyPerCap_limited = dfC_countyPerCap_limited.set_index("Admin2")
dfC_countyPerCap_limited = dfC_countyPerCap_limited.drop(columns = ["Combined_Key", "Population", "Province_State"])
dfC_countyPerCap_limited = dfC_countyPerCap_limited.transpose()
dfC_countyPerCap_limited.to_csv("US_CT_TimeSeries_COVID19_ConfirmedPer100k.csv", index_label="Date")
dfC_countyPerCap_limited = dfC_countyPerCap_limited.rename(columns={
    'Fairfield': 'Fairfield Confirmed per 100k', 
    'Hartford': 'Hartford Confirmed per 100k', 
    'Litchfield': 'Litchfield Confirmed per 100k', 
    'Middlesex': 'Middlesex Confirmed per 100k', 
    'New Haven': 'New Haven Confirmed per 100k', 
    'New London': 'New London Confirmed per 100k', 
    'Tolland': 'Tolland Confirmed per 100k', 
    'Windham': 'Windham Confirmed per 100k'})

dfC_countyPerCap_limited
#df_USPop_county

### End of US county confirmed per 100k residents time series


# In[51]:


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


# In[52]:


### Start of US county deaths per 100k residents time series

dfD_countyPerCap = dfD_US_county.copy().transpose()
dfD_countyPerCap_temp = df_USPop_county.copy()
dfD_countyPerCap_temp = dfD_countyPerCap_temp[["Population"]]

dfD_countyPerCap = pd.merge(dfD_countyPerCap, dfD_countyPerCap_temp, left_index=True, right_index=True, how="inner")
dfD_countyPerCap = dfD_countyPerCap.div(dfD_countyPerCap["Population"], axis=0)
dfD_countyPerCap = dfD_countyPerCap.mul(100000, axis=0)

dfD_countyPerCap = dfD_countyPerCap.drop(columns = ["Population"])
dfD_countyPerCap = dfD_countyPerCap.transpose()
dfD_countyPerCap.to_csv("US_County_TimeSeries_COVID19_DeathsPer100k.csv", index_label="Date")

# Limiting Data size
dfD_countyPerCap_limited = dfD_countyPerCap.copy().transpose()
dfD_countyPerCap_limited = pd.merge(dfD_countyPerCap_limited, df_USPop_county, left_index=True, right_index=True, how="inner")
selectedStates = ['Connecticut']
dfD_countyPerCap_limited = dfD_countyPerCap_limited.loc[dfD_countyPerCap_limited['Province_State'].isin(selectedStates)]
dfD_countyPerCap_limited = dfD_countyPerCap_limited.set_index("Admin2")
dfD_countyPerCap_limited = dfD_countyPerCap_limited.drop(columns = ["Combined_Key", "Population", "Province_State"])
dfD_countyPerCap_limited = dfD_countyPerCap_limited.transpose()
dfD_countyPerCap_limited.to_csv("US_CT_TimeSeries_COVID19_DeathsPer100k.csv", index_label="Date")
dfD_countyPerCap_limited = dfD_countyPerCap_limited.rename(columns={
    'Fairfield': 'Fairfield Deaths per 100k', 
    'Hartford': 'Hartford Deaths per 100k', 
    'Litchfield': 'Litchfield Deaths per 100k', 
    'Middlesex': 'Middlesex Deaths per 100k', 
    'New Haven': 'New Haven Deaths per 100k', 
    'New London': 'New London Deaths per 100k', 
    'Tolland': 'Tolland Deaths per 100k', 
    'Windham': 'Windham Deaths per 100k'})

df_countyPerCap_limited_final = pd.merge(dfC_countyPerCap_limited, dfD_countyPerCap_limited, left_index=True, right_index=True, how="inner")
#df_countyPerCap_limited_final.to_csv("US_CTCombined_TimeSeries_COVID19_DeathsPer100k.csv", index_label="Date")

df_countyPerCap_limited_final

#dfD_countyPerCap_limited
#dfD_countyPerCap
#df_USPop_county
### End of US county deaths per 100k residents time series


# In[53]:


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


# Population
df5 = df_USPop_State.copy()
#df5 = df5.drop(df5.columns[0:-1], axis=1)
#df5.columns = ['Deaths per 100K']


# Cross section of current state data merging
df_cross_section = pd.merge(df1, df2, left_index=True, right_index=True, how="inner")
df_cross_section = pd.merge(df_cross_section, df3, left_index=True, right_index=True, how="inner")
df_cross_section = pd.merge(df_cross_section, df4, left_index=True, right_index=True, how="inner")
df_cross_section = pd.merge(df_cross_section, df5, left_index=True, right_index=True, how="inner")


df_cross_section.to_csv("US_States_CrossSection_COVID19_All.csv")

df_cross_section
#df1
#df2
#df3
#df4
#df5
### End of US states cross section of latest data


# In[54]:


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


df_cross_section.to_csv("US_County_CrossSection_COVID19_All.csv")


#df1
#df_USPop_county
df_cross_section


# In[55]:


get_ipython().system('jupyter nbconvert --to script New_COVID_Data_Cleaning_Script.ipynb')

