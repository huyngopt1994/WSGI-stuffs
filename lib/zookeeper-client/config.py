# The library to get config from node of zookeeper
from collections import OrderedDict
import json
import zlib

def get_config(log, zk, zk_path, dump=True):
    
    conf, stat = zk.get(zk_path)
    
    if data is not None:
        data = json.loads(zlib.decompress(data).decode('utf-8')),
                          object_pairs_hook=OrderedDict)
    
    return data
