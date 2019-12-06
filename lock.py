'''
Created on Nov 28, 2019

@author: Ian Lam, Yu Ting Chiu
'''


class Lock(object):
    '''
    Lock locks a data item. A transaction needs to acquire a lock before it can access the data item.
    
    Attributes: 
        lock_type (string): Type of Lock - "SL"/"XL"
        txn_holding (list(int) ): List of transaction IDs that are sharing/holding this lock
        site_id (int): ID of site of where the lock is located at.
        item_locked (Variable Object): Item locked by this lock
    '''
    def __init__(self, lock_type, txn_id, site_id, item):
        '''
        Constructor
        
        Parameters:
            lock_type (string): Type of Lock - "SL"/"XL"
            txn_id (int): ID of transaction that holds lock
            site_id (int): ID of site of where the lock is located at.
            item (Variable Object): Item locked by this lock
        '''
        self.lock_type = lock_type
        self.txn_holding = [txn_id]
        self.site_id = site_id
        self.item_locked = item
