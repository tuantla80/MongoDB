from pymongo import MongoClient

def get_current_max_user_id_in_all_dbs(
        host,
        port,
        field_name='UserID',
        ignore_db_names=['admin', 'local', 'config']):
    '''
    Purpose: To get current max UserID in all databases
    INPUT:
        host:
        port:
        field_name: a field in a document to get max UserID
        ignore_db_names: list of DB to be ignored.
    OUTPUT:
        max_user_id: integer value
        
    NOTE: Since we write "UserID" in increase order so the last
    documents in a collection always has its max UserID.
    Example: UserID = PID0000000009 so user_id = int(0000000009)=9
    
    Author: TLA. Tuan
    '''
	
    mongodb = MongoClient(host, port)
    db_names = mongodb.list_database_names()  
    db_names = set(db_names) - set(ignore_db_names)
    
    max_user_id = 0
    for db_name in db_names:
        _db = mongodb[db_name]
        for collection_name in \
                _db.list_collection_names(include_system_collections=False):
            last_doc = _db[collection_name].find({}) \
                                           .sort("_id", -1) \
                                           .limit(1)
            last_doc = [v for v in last_doc]
            if last_doc and field_name in last_doc[0].keys():
                this_max_user_id = last_doc[0][field_name]
                this_max_user_id = int(this_max_user_id[3:])  # [3:] to remove PID word
                if this_max_user_id > max_user_id:
                    max_user_id = this_max_user_id
    # End of for db_name in dbs
    return max_user_id
	
if __name__ == '__main__':
	host = '127.0.0.1'
	port = 27017
	max_user_id = get_current_max_user_id_in_all_dbs(host, port)
	print('max_user_id = {}'.format(max_user_id))  # Ex. max_user_id = 9090909090
