Pandas Join:
This is one technique of joining multiple pandas datframe.
create a list of all the pandas dataframe to be formatted
and then convert it into pyspark dataframe.
frames = [fstorefile[f] for  f in files]
 from qground_truth import error_module
store_all=pd.DataFrame(["imsi"])
store_list=[None]*10
import pandas as pd
for itr in range(10):
    store_list[itr] = imsi_module.imsi_subsets(gdat,sdf,20,72000,0.01).toPandas() #create a list and append the list
store_all = pd.concat(store_list) #take  the list and concatenated all pandas dataframe

Pandas filtering for a column:
gdat_cache = gdat[(gdat['timestampMs'] >= date_timestamp("07/01/2021")) & (gdat['timestampMs'] <= date_timestamp("07/02/2021"))]

Pandas groupby:




Pyspark join
define a data frame say store_all where you want to join the dataframe  in the form of list

from functools import reduce
rom pyspark.sql import DataFrame
Map-applies a function for all elements in the list.
map(function to apply, list of inputs)
eg sqauring elements in a list
items = [1, 2, 5, 9]
square = map(lambda x:  x**2, items)


Filter: help us to create a list where the elements in the list are satisfying certain condition
list1 [-5 ,1, 3,4,6,-9]
filter(lambda x: x<0 , list1)

Reduce: used to perform computational operatation  within the elements of the list.

Use reduce(lambda x , y: x.union(y), store_list)
Pyspark groupBy:

sdf.groupBy("imsi").count()- this helps to  group the imsi based on the count.
to arrange the rows in descending order
imsi_top = obtain_imsi.sort(F.col('count').desc()).limit(5)
______________________
