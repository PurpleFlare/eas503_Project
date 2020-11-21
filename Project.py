# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 09:54:59 2020

@author: Sam
"""

import pandas as pd
import numpy as np

#change your file path
homeless = pd.read_csv('C:/Users/Sam/Documents/2020Fall/EAS503/Code/Project/homeless_impact.csv')
print(homeless.head())
print(homeless['county'].nunique())
print(homeless['county'].value_counts())

#change your file path
hospital = pd.read_csv('C:/Users/Sam/Documents/2020Fall/EAS503/Code/Project/hospitals_by_county.csv')
print(hospital.head())