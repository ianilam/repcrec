'''
Created on Nov 26, 2019

@author: ian
'''

class Transaction(object):

    def __init__(self, t_id, ts):
        '''
        Constructor
        '''
        self.id = t_id
        self.current_instruction = ""
        self.timestamp = ts
        self.status = "running"
    

class ReadWrite_Transaction(Transaction):
    
    def __init__(self, t_id, ts):
        
        super().__init__(t_id, ts)
        self.transaction_type = "read_write"
        self.locks_holding = []
        self.cache = {}
        
        
        
class ReadOnly_Transaction(Transaction):
    
    def __init__(self, t_id, ts):
        super().__init__(t_id, ts) 
        self.transaction_type = "read_only"
        self.snapshot = {}
        # grab the snapshot in all the variable's values at that moment
        