'''
Created on Nov 26, 2019

@author: ian
'''

class Transaction(object):
    '''
    classdocs
    '''


    def __init__(self, t_id, t_type, ts):
        '''
        Constructor
        '''
        self.id = t_id
        self.status = "created"
        self.transaction_type = t_type
        self.locks_holding = []
        self.current_instruction = ""
        self.timestamp = ts
        