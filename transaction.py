'''
Created on Nov 26, 2019

@author: Ian Lam, Yu Ting Chiu
'''


class Transaction(object):
    """
    Transaction is a unit of work that has a number of operations on a database. 
    
    Attributes:
        id (int): ID of transaction
        current_instruction (string): current instruction that it's running
        timestamp (int): timestamp of when the transaction started
        status (string): status of transaction - "running"/"blocked"/"committed"/"aborted"
    """
    def __init__(self, t_id, ts):
        '''
        Constructor
        
        Parameters:
            t_id (int): Transaction ID
            ts (int): Timestamp of when the transaction began
        '''
        self.id = t_id
        self.current_instruction = ""
        self.timestamp = ts
        self.status = "running"


class ReadWrite_Transaction(Transaction):
    """
    ReadWrite_Transaction inherits from Transaction. It is constructed when a "begin" instruction is read.
    
    Attributes:
        id (int) --- ID of transaction
        current_instruction (string) --- current instruction that it's running
        timestamp (int) --- timestamp of when the transaction started
        status (string) --- status of transaction - "running"/"blocked"/"committed"/"aborted"
        transaction_type (string) --- Type of transaction
        locks_holding (list (Locks Object) ) --- List of Lock objects the transaction holds
        cache (dict (int: tuple) ) ---  timestamp: (read/write actions)
        uncommit_values (dict (int: int)) --- item_id: the uncommitted newest value
    """
    def __init__(self, t_id, ts):
        '''
        Constructor
        
        Parameters:
            t_id (int): Transaction ID
            ts (int): Timestamp of when the transaction began
        '''

        super().__init__(t_id, ts)
        self.transaction_type = "read_write"
        self.locks_holding = []
        self.cache = {}
        self.uncommit_values = {}


class ReadOnly_Transaction(Transaction):
    """
    ReadOnly_Transaction inherits from Transaction. It is constructed when a "beginRO" instruction is read.

    Attributes:
        id (int) --- ID of transaction
        current_instruction (string) --- current instruction that it's running
        timestamp (int) --- timestamp of when the transaction started
        status (string) --- status of transaction - "running"/"blocked"/"committed"/"aborted"
        transaction_type (string) --- Type of transaction
        snapshot (dict (int: int) ) --- snapshot of database when this ReadOnly_Transaction begins. {item id: item value}
    """
    def __init__(self, t_id, ts):
        '''
        Constructor
        
        Parameters:
            t_id (int): Transaction ID
            ts (int): Timestamp of when the transaction began
        '''

        super().__init__(t_id, ts)
        self.transaction_type = "read_only"
        self.snapshot = {}
