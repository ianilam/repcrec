'''
Created on Nov 26, 2019

@author: ian
'''
from transaction import ReadWrite_Transaction, ReadOnly_Transaction
from site_manager import Site_Manager

class Transaction_Manager(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        self.txns = {}
        self.tick = -1
        self.site_manager = Site_Manager()

    
    def read_instruction(self, instr):
        self.tick += 1
        instr = instr.replace(" ", "")
        instruction = instr[:-1].split("(")
        op = instruction[0]
        arg = instruction[1]
        if op == 'begin':
            txn = arg[1:]
            self.begin(instr, int(txn))
         
        elif op == 'beginRO':
            txn = arg[1:]
            self.beginRO(instr, int(txn))
         
        elif op == 'R':
            txn, item = arg.split(",")
            self.read(instr, int(txn[1:]), int(item[1:]))
#             if self.txns[int(txn[1:])].status == "blocked":
#                 self.txns[int(txn[1:])].pending_operations.append(instr)
#             else:
#                 self.read(instr, int(txn[1:]), int(item[1:]))
#                     
        elif op == 'W':
            txn, item, new_value = arg.split(",")
            self.write(instr, int(txn[1:]), int(item[1:]), int(new_value))
#             if self.txns[int(txn[1:])].status == "blocked":
#                 self.txns[int(txn[1:])].pending_operations.append(instr)
#             else:
#                 self.write(instr, int(txn[1:]), int(item[1:]), int(new_value))
         
        elif op == 'fail':
            site_id = int(arg)
            self.fail(int(site_id))
         
        elif op == 'recover':
            site_id = int(arg)
            self.recover(int(site_id))
        
        elif op == 'end':
            txn = arg
            self.end(instr, int(txn[1:]))
#             if self.txns[int(txn[1:])].status == "blocked":
#                 self.txns[int(txn[1:])].pending_operations.append(instr)
#             else:
#                 self.end(instr, int(txn[1:]))
        
        elif op == 'dump':
            self.dump()
        
        else:
            print("invalid instruction")
            
    
    def begin(self, instr, txn_id):
        print("begin Transaction", txn_id, "at timestamp", self.tick)
        txn = ReadWrite_Transaction(txn_id, self.tick)
        txn.current_instruction = instr
        self.txns[txn_id] = txn
        
        # if read only
    def beginRO(self, instr, txn_id):
        print("begin ReadOnly Transaction", txn_id, "at timestamp", self.tick)
        txn = ReadOnly_Transaction(txn_id, self.tick)
        txn.current_instruction = instr
        self.txns[txn_id] = txn
#         acquire snapshot from site manager
        txn.snapshot = self.site_manager.acquire_snapshot()
        print(f'Snapshot Acquired at timestamp {self.tick}:\n{txn.snapshot}')
    
    
    # txn_id int
    # item int    
    def read(self, instr, txn_id, item_id):
        print(f"Transaction {txn_id} wants to read x{item_id} at timestamp {self.tick}")
        txn = self.txns[txn_id]
        txn.current_instruction = instr
        
        if txn.transaction_type == "read_write":
            share_lock = self.site_manager.acquire_share_lock(txn_id, item_id)
            if share_lock!= None:
                self.txns[txn_id].locks_holding.append(share_lock)
#                 read_item = self.site_manager.read(share_lock.site_id, item_id)
                read_item_value = share_lock.item_locked.value
                print(f'x{item_id}: {read_item_value}')
                txn.cache[self.tick] = ["read", (item_id, read_item_value, [share_lock.site_id])]
            else:
                txn.status = "blocked"
        
        if txn.transaction_type == "read_only":
            read_item_value = txn.snapshot[item_id]
            if read_item_value != None:
                print(f'x{item_id}: {read_item_value}')
            else:
                print(f'Not able to read x{item_id} due to site failures.')
        
    
    def write(self, instr, txn_id, item_id, new_value):
        print(f'Transaction {txn_id} wants to write to x{item_id} with {new_value} at timestamp {self.tick}')
        # first need to figure out where the item is at
        txn = self.txns[txn_id]
        txn.current_instruction = instr
                
        # go acquire exclusive locks for the item
        exclusive_locks = self.site_manager.acquire_exclusive_lock(txn_id, item_id)
        if len(exclusive_locks) > 0:
            locked_sites = []
            for l in exclusive_locks:
                if l not in txn.locks_holding:
                    txn.locks_holding.append(l)
                locked_sites.append(l.site_id)
            txn.cache[self.tick] = ["write", (item_id, new_value, locked_sites)]
        else:
            txn.status = "blocked"

    def fail(self, site_id):
        self.site_manager.fail(site_id, self.tick)
        
        
    def recover(self, site_id):
        self.site_manager.recover(site_id, self.tick)
    
    
    def dump(self):
        print("dumping stuff", "at timestamp", self.tick)
        self.site_manager.dump()
        # dump out all the committed values
    
    def end(self, instr, txn_id):
        print("Ending Transaction", txn_id, "at timestamp", self.tick)
        txn = self.txns[txn_id]
        txn.current_instruction = instr
        
        if txn.transaction_type == "read_write":
            print(f'Transaction {txn_id} \'s operations were:')
            print(txn.cache)
            # need to check if everything is okay or not
            if self.site_manager.check_commit(txn.cache):
                print(f'Transaction {txn_id} is okay to commit!')
                self.site_manager.commit(txn)
                txn.status = "committed"
            else:
                print(f'Transaction {txn_id} is going to abort :(')
                self.site_manager.abort(txn)
                txn.status = "aborted"
        
        if txn.transaction_type == "read_only":
            print("Transaction", txn_id, "ended.")
            txn.status = "committed"

        self.site_manager.txns_ready_list = self.site_manager.txns_ready_list + self.site_manager.txns_waiting_list
        self.site_manager.txns_waiting_list = []

        
    def run_ready_transactions(self):
        while len(self.site_manager.txns_ready_list) > 0:
            print()
            ready_txn_id = self.site_manager.txns_ready_list.pop(0)
            print(f'Resume Transaction {ready_txn_id}')
            print(f'Transaction {ready_txn_id} goes from {self.txns[ready_txn_id].status} to running')
            print(self.txns[ready_txn_id].current_instruction)
            self.txns[ready_txn_id].status = "running"
            self.read_instruction(self.txns[ready_txn_id].current_instruction)
            print()
    
    def find_cycle(self):
        cycles = self.site_manager.find_cycle()
        if len(cycles) > 0:
        
            for cycle in cycles:
                print(f'Cycle detected in Transactions {cycle}')
                self.kill_youngest(cycle)
    
    def kill_youngest(self, cycle):
        cycle.sort(key=lambda txn_id: self.txns[txn_id].timestamp, reverse=True)
        youngest_txn = self.txns[cycle[0]]
        print(f'Killing the youngest Transaction {cycle[0]}')
        youngest_txn.status = "aborted"
        self.site_manager.kill(youngest_txn)
        