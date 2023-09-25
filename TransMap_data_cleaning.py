import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2

#connect to transmap postgres instance
#Important: Don't commit this file to public git (or anywhere public), without shielding credential in secrets file.
psql_conn = psycopg2.connect(host = "compass.cast.uark.edu",user = "transmapRead",password="2ogHwuEbJi7n0UfxAiz5YrtUCcqXzF",database="transmap")
cur = psql_conn.cursor()

#query database for historical_active_stoppages table and store table in dataframe
cur.execute("SELECT * FROM historical_active_stoppages")
df = pd.DataFrame(cur.fetchall())

#query and apply column labels
cur.execute("Select * FROM historical_active_stoppages LIMIT 0")
colnames = [desc[0] for desc in cur.description]
print(colnames)
df.columns = colnames

#implement cleaning procedure I used on the original extract on 2-17-2022
#create unique chamber identifier
df["unique_chamber_id"] = df["locknumber"].astype('str') + df["rivercode"].astype('str') + df["chambernumber"].astype('str')

#get all chamber identifiers
unique_id = pd.unique(df["unique_chamber_id"])

results_df = pd.DataFrame()
#look at all chambers
i = 0
for id in unique_id:
    current_chamber_df = df[df["unique_chamber_id"] == id]
    if(i == 0):
        results_df = current_chamber_df.drop_duplicates(subset="beginstopdate", ignore_index=True)
    else:
        results_df = pd.concat([results_df, current_chamber_df.drop_duplicates(subset="beginstopdate", ignore_index=True)] )
    i = i + 1
#unique location ID
results_df["unique_id"] = results_df["locknumber"].astype('str') + results_df["rivercode"].astype('str')

#calculate duration of stoppage
results_df["duration hrs"] = results_df["endstopdate"].astype("datetime64") - results_df["beginstopdate"].astype("datetime64")

#force time to be expressed in hours
results_df["duration hrs"] = results_df["duration hrs"].astype("timedelta64[h]")

#Export results to CSV (to validate against the extract you are currently using
results_df.to_csv("historical_active_stoppages_cleaned.csv", index=False)

# #print cleaned dataframe to console
# print(results_df)

# #TODO: From here, you should be able to plugin whatever transformations you made on the original csv.
# #One Callout: the CSV column headers are camel case, when you extract from postgres storage they are all lowercase


#read the generated historical_active_stoppages_clean csv table
# TransMap = pd.read_csv('historical_active_stoppages_cleaned.csv')
TransMap = results_df
#enter the erocCode, riverCode, and lockNumber here
df = TransMap[(TransMap['eroccode'] == 'B2') & (TransMap['rivercode'] == 'GI') & (TransMap['locknumber'] == 1)]
# Replace Yes with 1, No with 0
df.isscheduled.replace(('Yes', 'No'), (1, 0), inplace=True)

# tranfer the beginstopdate and endstopdate as the datetime type
df['beginstopdate'] = pd.to_datetime(df['beginstopdate'])
df['endstopdate'] = pd.to_datetime(df['endstopdate'])
# print(type(df.beginStopDate[0]))
# Sort the data by datetime
df = df.sort_values(by='beginstopdate')

# find the max time between two adjacent lines
df_start1 = df.iloc[:-1, 4]
df_start2 = df.iloc[1:, 4]
df_start3 = pd.concat([df_start1.reset_index().drop('index', axis=1),df_start2.reset_index().drop('index', axis=1)], axis = 1)
df_start = df_start3.max(axis=1)
# find the min time between two adjacent line
df_end1 = df.iloc[:-1,5]
df_end2 = df.iloc[1:,5]
df_end3 = pd.concat([df_end1.reset_index().drop('index', axis=1),df_end2.reset_index().drop('index', axis=1)], axis = 1)
df_end = df_end3.min(axis=1)

# subtract begin time from end time 
dfinter = df_end - df_start
dfoverlap = dfinter.dt.total_seconds()
# if the substraction greater than 0, there is a overlap, if smaller than 0, there is no overlap
idx_g0=(dfoverlap.values).astype(np.float64) >= 0
idx_l0=(dfoverlap.values).astype(np.float64) < 0
dfoverlap.iloc[idx_g0] = 'overlap' 
dfoverlap.iloc[idx_l0] = 'no overlap' 

# Generate a column named multiple reasons.
# When overlap happens but their reason code are different, 
# then merge the overlap lines into one, put all the reasons in the "reasoncode", and put a binary variable 1 
# in the "Multiple reasons", if all the reasons are the same among overlap lines, 
# then 0 is put in the "Multiple reasons".
zero_data = np.zeros(shape=(len(df),1))
df_multiplereasons = pd.DataFrame(zero_data, columns=['Multiple reasons'])
df = df.reset_index()
df = pd.concat([df, df_multiplereasons], axis=1)
# find the index overlap happens
output_index = dfoverlap.iloc[idx_g0].index
dfindex = pd.DataFrame(output_index)
dfindex = dfindex + 1
# find the row index of overlaps
# idx_np = dfindex.values
idx_np = output_index.values
idx_np = np.repeat(idx_np, 2)
idx_np[1::2] = idx_np[1::2] + 1
df_overlap = df.iloc[list(idx_np.squeeze()), :]
# define a function that compare if the reason code of two adjacent rows are same or not.
# if they are the same, merge two rows, if they are different, concat their reasons
def diff(df, window=1):   
    df_odd = df[0::2]
    df_even = df[1::2]
    df_out = df_odd.copy()
    idx = np.where(df_odd.values != df_even.values)[0]
    df_out.values[idx] += "/" + df_even.values[idx]
    return df_out
def diff2(df, window=1):
    shifted_df = df.shift(window)
    shifted_df = shifted_df.fillna(df)      
    return (df == shifted_df).iloc[1::2]
#Show if the overlap lines are different or not 
diff2(df_overlap[['unique_chamber_id','numhwcycles', 'reasoncode', 'isscheduled']])
# Convert true to false, false to true
value = diff2(df_overlap['reasoncode'])
value ^= True
# Convert true to false, false to true
value2 = diff2(df_overlap['isscheduled'])
value2 ^= True
# generate a list with number 2
index_multipSche = value2.index[value2.values]
l = [2] * (len(index_multipSche))
# replace number in "isScheduled" with 2 
df.iloc[list(index_multipSche-1), 9] = l
dfindex_1 = dfindex -1
df.iloc[dfindex_1.squeeze(), 10] = diff(df_overlap['reasoncode'])
df.iloc[dfindex_1.squeeze(), 17] = value.astype(int).values
df['merge'] = pd.Series([0 for x in range(len(df.index))])
df['merge'].iloc[list(idx_np.squeeze())] = 1
## Merge overlaps among all rows
index1 = []
for j in range(len(df)-1):
    
    if df.endstopdate.iloc[j] > df.beginstopdate.iloc[j+1]:
        index1.append(j)
        df.beginstopdate.iloc[j+1] = min(df.beginstopdate.iloc[j],df.beginstopdate.iloc[j+1])
        df.endstopdate.iloc[j+1] = max(df.endstopdate.iloc[j],df.endstopdate.iloc[j+1])
df = df.drop(index=index1)

# add a new column "broken", when "broken" = 1, it is downtime, when "broken" = 0, it is uptime.
df = df.assign(broken=1)
df_1 = df['beginstopdate'].iloc[1::]
df_2 = df['endstopdate'].iloc[0:-1]
df = df.iloc[np.repeat(np.arange(len(df)), 2)]
df1 = df[1::2]
df_1 = df_1.reset_index()
df_2 = df_2.reset_index()
df_1 = df_1['beginstopdate']
df_2 = df_2['endstopdate']
df1 = df1.reset_index()
df1['beginstopdate']=df_2
df1['endstopdate']=df_1
df1 = df1.drop(['index'], axis=1)
df[1::2] = df1
df['broken'].iloc[1::2] = 0
df = df.drop(['index'], axis=1)
df = df[:-1]
df['Duration_time'] = (df['endstopdate'] - df['beginstopdate']).dt.total_seconds() / 60 / 60
df['Duration_time'].iloc[:-1]
df = df.reset_index()
df['duration hrs'] = df['Duration_time'].iloc[:-1].astype(int)
df = df[['duration hrs','isscheduled', 'broken', 'reasoncode']]

df1 = df[(df['broken'] == 1)]
df2 = df[(df['broken'] == 0)]
df1 = df1[['duration hrs','isscheduled', 'reasoncode']]
df2 = df2['duration hrs']
df3 = df1[['duration hrs','isscheduled']]
df1 = df1.iloc[1:]

new_row = {'duration hrs':'NA', 'isscheduled': 'NA', 'reasoncode':'NA'}
df1 = df1.append(new_row, ignore_index=True)
df1 = df1.reset_index()
df1 = df1.drop(['index'], axis=1)
df2 = df2.reset_index()
df2 = df2.drop(['index'], axis=1)
df3 = df3.reset_index()
df3 = df3.drop(['index'], axis=1)
df1 = df1.rename(columns={"duration hrs": "downtime", "isscheduled": "scheduled"})
df2 = df2.rename(columns={"duration hrs": "pre_uptime"})
df3 = df3.rename(columns={"duration hrs": "pre_downtime", "isscheduled": "pre_scheduled"})
frames = [df1, df2, df3]
df = pd.concat(frames,axis = 1)
df = df[:-1]
df = df[:-1]
    
# writing to csv

df.to_csv('Analysisprepared.csv')