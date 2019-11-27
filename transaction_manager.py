'''
Created on Nov 26, 2019

@author: ian
'''
from transaction import Transaction

class Transaction_Manager(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.txns = []
        self.tick = 0
    
    def begin(self, txn_id, txn_type):
        txn = Transaction(txn_id, txn_type, self.tick)
        self.txns.append(txn)
        
    def read(self, txn_id):
        pass
    
    def write(self, txn_id, data_item, new_value):
        pass
    
    def dump(self):
        pass
    
    def end(self, txn_id):
        pass