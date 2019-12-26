"""
Purpose: To build Python code for upload to and download from MongoDB
"""
import sys
import os
import pandas as pd
from pymongo import MongoClient


class MongoDB:
    """
    To access, upload and download to MongoDB
    """
    def __init__(self,
                 host,
                 port,
                 db_name=None,
                 collection_name=None):  
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name

    def get_client(self):
		""" To get mongo client object """
        return MongoClient(self.host, self.port)

    def get_db(self):
        """ To get DB object of the current db """
        return self.get_client()[self.db_name]

    def get_dbs(self):
        """ To get list of DB in MongoDB including system DB """
        mc = MongoClient(self.host, self.port)
        return mc.list_database_names()

    def get_collection(self):
        """ To get collection object of current collection """
        return self.get_db()[self.collection_name]

    def get_list_collection_names(self):
        """ To get list of all collection in the currrent db object"""
        _db = self.get_db()
        return _db.list_collection_names(include_system_collections=False)

    def upload_df(self, df):
        """
        To upload a df (pandas dataframe) to MongoDB
        """
        self.get_db()[self.collection_name].insert_many(df.to_dict('record'))
        return None

    def upload_dics(self, dics):  
		""" To upload a list of dics to MongoDB 
			Example: dics = [dic1, dic2,..., dicN]
		"""
	    self.get_db()[self.collection_name].insert_many(dics)
        return None

    def download_df(self, query=None, limit=None, no_id=True, projection=None):
        """
        Read from MongoDB and Store it into a df (DataFrame)
        Ex. limit = 100  # number of documents
		    projection = {'UserID': 1, 'Name': 1} where 1 here means True.            
        """
        query = query or {}
        projection = projection or {}

        _db = self.get_db()
        if not limit:
            if projection:
                cursor = _db[self.collection_name].find(query, projection)
            else:
                cursor = _db[self.collection_name].find(query)
        else:
            if projection:
                cursor = _db[self.collection_name].find(query, projection)\
                                                  .limit(limit)
            else:
                cursor = _db[self.collection_name].find(query).limit(limit)
        # End of if not limit
        df = pd.DataFrame(list(cursor))
        # Delete the _id
        if no_id:
            del df['_id']
        return df
		
		
class Index(MongoDB):
	""" Purpose: To making index for MongoDB """
    def create_single_index(self, keys):
        """
        Ex. keys=['mfp.idx', 'mfp.count']
        """
        assert (isinstance(keys, list) and
                all(isinstance(key, str) for key in keys))
        for key in keys:
            self.get_collection().create_index(key)
            # NOTE: ensure_index deprecated
        # End of for
        return None

    def create_compound_index(self, keys):
        """
        Ex. keys = [('pfp.idx', 1), ('pfp.value', 1)]
        """
        assert (isinstance(keys, list) and
                all(isinstance(key, tuple) for key in keys))

        self.get_collection().create_index(keys)
        return None

    def create_text_index(self, keys):
        """
        self.keys = [('pfp_str.idx', 'text')]
        """
        self.get_collection().create_index(keys)
        return None

    def drop_indexes(self, keys):
        """
        Note: create index may take much time BUT drop index is very fast
        Ex. keys = ['pattern_fp.bit_ids_1', 'pattern_fp.count_1']
        """
        for _index in keys:
            self.get_collection().drop_index(_index)
        return None

    def calculate_index_size(self):
        return self.get_db().command("serverStatus")
		
		
class Management(MongoDB):
	""" Purpose: To manage MongoDB 
		This class will be updated more
	"""
    def remove_field_from_document(self, fields=None):
        """
        Ex. remove_field_from_document(collection, fields=['pfp_', 'pfp_4'])
        """
        fields = fields or []

        for field in fields:
            self.get_collection().update_many({},
                                              {"$unset": {field: 1}})
        return None

    def find_document_in_a_collection(self, query=None):
        query = query or {}
        return self.get_collection().find(query)

    def rename_field_name(self, dic_rename=None):
        """
        Usage:
            Management is inherited from MongoDB class
            Management(host=HOST,
                       port=PORT,
                       db_name=DB_NAME,
                       collection_name=COLLECTION_NAME)
					   .rename_field_name(dic_rename={'chemicalID': 'mol_id'})
        """
        dic_rename = dic_rename or {}

        _db = self.get_db()
        _db[self.collection_name].update_many({}, {'$rename': dic_rename})
        return None