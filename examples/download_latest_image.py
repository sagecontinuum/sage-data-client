"""
Created on Thu Dec 22 08:34:21 2022
@author: bhupendra
"""

import sage_data_client
import os
import urllib


# 1. Checks Sage camera  images from last one hour and downloads. 
# 2. Suitable to run in loop or as cronejob.
# 3. It will only download the new images avoiding repeat downloads.
# 4. You can chnage the settings below to change the camera, node and period.


#Download setting
node = "W021"
cam_stream = 'imagesampler-top' #'imagesampler-bottom'
start_time = "-1h"

# Set output dir
data_dir = '/Users/bhupendra/data/sage_data/'
data_dir = data_dir+node+'/'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


#Get the dataframe containing image metadata
df = sage_data_client.query(
    start=start_time,
    filter={
        "plugin": "registry.sagecontinuum.org/theone/imagesampler:0.3.0",
        "vsn": node
    }
)

#Sort for latest images
df=df.loc[df['meta.job']==cam_stream]
df.sort_values(by='timestamp', inplace = True, ascending = False)

#extract URLs
targets = []
times = []
for i in range(len(df)):
    if 'jpg' in df.iloc[i].value:
        targets.append(df.iloc[i].value)
        times.append(df.iloc[i].timestamp)


#Download
for i in range(len(times)): 
    link = targets[i]
    fname_str = node+'_'+cam_stream+'_%Y%m%d-%H%M%S'+'.jpg'
    filename = os.path.join(data_dir,times[i].strftime(fname_str))
    if not os.path.exists(filename):
        urllib.request.urlretrieve(link, filename)
        print('Donwloading '+filename)
    
    
    
    
    
    
    
    
    
    
    
    
