import glob
import re
import json
#import concurrent.futures as futures
import os

test_dir = "/home/thisguy/data/enwiki20201020/"
test_file = test_dir + "0aaf9aae-5557-466c-8c50-13170a3cbec8.json"

# files = [f for f in glob.glob(test_dir+"**")]

# def clean_file(file):
#     fn = os.path.basename(file)
#     dir_name = os.path.dirname(file)
#     new_fn = fn.split(".")[0] + "-modified" + ".json" 
#     new_fp = os.path.join(dir_name, new_fn)
    
#     with open(file, "r") as in_json, open(new_fp, "w") as out_json:
#         data = json.load(in_json)
#         json.dump(data, out_json)

# def multiprocess_clean():
#     with futures.ProcessPoolExecutor() as pool:
#         task_to_filename = {pool.submit(clean_file, f):f for f in files}
#         for f in futures.as_completed(task_to_filename):
#             try:
#                 f.result()
#             except Exception as e:
#                 filename = task_to_filename[f]
#                 print(f"{filename} processing failed: {e}")
 
# multiprocess_clean()   

'''
Notes on text content:


Category: -> denotes categories at end of text.
== See also == -> related topics


'''

from dask.distributed import Client, progress, LocalCluster
import dask.bag as dbag

cluster = LocalCluster()
client = Client(cluster)

mod_files = [f for f in glob.glob(test_dir+"**") if "modified" in f]

bag = dbag.read_text(mod_files[0:100]).map(json.loads)
example = bag.take(10)

example_text = example[0][104]['text']

'''
#need to filter categories

'Category:' - seen at end of text field. 

cats = re.split("Category\:")
if cats:
    categories = [item.strip() for item in cats[1:]]
elif not cats:
    categories = []
    
'''

category_regex = re.compile('Category\:()')