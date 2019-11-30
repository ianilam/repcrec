'''
Created on Nov 28, 2019

@author: ian
'''

class Lock(object):
    '''
    classdocs
    '''


    def __init__(self, lock_type, txn_id, site_id, item):
        '''
        Constructor
        '''
        self.lock_type = lock_type
        self.txn_holding = []
        self.site_id = site_id
        self.item_locked = item
        
        self.txn_holding.append(txn_id)

    
    def getInfo(self):
        return f'Lock Type: {self.lock_type}, Txns Holding Lock: {self.txn_holding}, Site ID: {self.site_id}, Item Locked: {self.item_locked.id}'