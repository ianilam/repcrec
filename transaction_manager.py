'''
Created on Nov 26, 2019

@author: Ian Lam, Yu Ting Chiu
'''

from transaction import ReadWrite_Transaction, ReadOnly_Transaction
from site_manager import Site_Manager


class Transaction_Manager(object):
    """
    Transaction_Manager is responsible for parsing inputs and delegating any operation that requires site interaction to the site manager.
    
    Attributes:
        txns (dict(int: Transaction Object) ): mapping of transaction_id to transaction object
        tick (int): clock for the Transaction_Manager
        site_manager (Site_Manager Object): site_manager that handles data item operations and site events

    """
    def __init__(self):
        '''
        Constructor
        '''
        self.txns = {}
        self.tick = -1
        self.site_manager = Site_Manager()

        self.debug = False

    def read_instruction(self, instr):
        """ Reads instructions and translates to a corresponding Transaction_Manager method
    
        Parameters:
            instr (string): Instruction received from input file or standard input
            
        Side Effect:
            Each time this function is called, the clock of the Transaction_Manager increases by 1
        """

        self.tick += 1
        print(f'tick {self.tick}: {instr}')

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

        elif op == 'W':
            txn, item, new_value = arg.split(",")
            self.write(instr, int(txn[1:]), int(item[1:]), int(new_value))

        elif op == 'fail':
            site_id = int(arg)
            self.fail(int(site_id))

        elif op == 'recover':
            site_id = int(arg)
            self.recover(int(site_id))

        elif op == 'end':
            txn = arg
            self.end(instr, int(txn[1:]))

        elif op == 'dump':
            self.dump()

        else:
            print("invalid instruction")

    def begin(self, instr, txn_id):
        """ Begins a read-write transaction and adds the new Transaction Object to txns
    
        Parameters:
            instr (string): The original instruction string.
            txn_id (int): ID of transaction
        
        """

        if self.debug:
            print("begin Transaction", txn_id, "at timestamp", self.tick)
        txn = ReadWrite_Transaction(txn_id, self.tick)
        txn.current_instruction = instr
        self.txns[txn_id] = txn

    def beginRO(self, instr, txn_id):
        """Begins a read-only transaction, adds the new Transaction Object to txns, acquires snapshot of database
    
        Parameters:
            instr (string): The original instruction string.
            txn_id (int): ID of transaction
            
        """

        if self.debug:
            print("begin ReadOnly Transaction", txn_id, "at timestamp",
                  self.tick)
        txn = ReadOnly_Transaction(txn_id, self.tick)
        txn.current_instruction = instr
        self.txns[txn_id] = txn

        # acquire snapshot from site manager
        txn.snapshot = self.site_manager.acquire_snapshot(txn_id)
        if txn.snapshot != None:
            if self.debug:
                print(
                    f'Snapshot Acquired at timestamp {self.tick}:\n{txn.snapshot}'
                )
        else:
            txn.status = "blocked"
            print(
                f'Transaction {txn_id} unable to acquire snapshot. Need to wait.'
            )

    def read(self, instr, txn_id, item_id):
        """ If transaction is ReadWrite, asks Site_Manager to acquire shared lock for item, if successful then read item from database
        If transaction is ReadOnly, read from snapshot
    
        Parameters:
            instr (string): The original instruction string.
            txn_id (int): ID of transaction
            item_id (int): ID of item to be read
        """

        if self.debug:
            print(
                f"Transaction {txn_id} wants to read x{item_id} at timestamp {self.tick}"
            )
        txn = self.txns[txn_id]
        txn.current_instruction = instr

        if txn.transaction_type == "read_write":
            # go acquire shared lock from site manager
            share_lock = self.site_manager.acquire_share_lock(txn_id, item_id)

            if share_lock != None:
                # check if the returned lock is one that transaction already has (due to lock promotion)
                if share_lock not in self.txns[txn_id].locks_holding:
                    self.txns[txn_id].locks_holding.append(share_lock)

                # check if transaction is reading from an item it has written earlier
                if item_id in txn.uncommit_values:
                    read_item_value = txn.uncommit_values[item_id]
                else:
                    read_item_value = share_lock.item_locked.value

                print(f'x{item_id}: {read_item_value}')

                # add this operation to the transaction's cache
                txn.cache[self.tick] = [
                    "read", (item_id, read_item_value, [share_lock.site_id])
                ]

            else:
                txn.status = "blocked"

        if txn.transaction_type == "read_only":
            # read from snapshot
            read_item_value = txn.snapshot[item_id]
            if read_item_value != None:
                print(f'x{item_id}: {read_item_value}')

    def write(self, instr, txn_id, item_id, new_value):
        """ Acquires exclusive locks for the data item. If successful, records this action to transaction's cache
        This doesn't actually write to the actual database, since we only write when we can commit

        Parameters:
            instr (string): The original instruction string.
            txn_id (int): ID of transaction
            item_id (int): ID of item to be written
            new_value (int): new value of item
        """

        if self.debug:
            print(
                f'Transaction {txn_id} wants to write to x{item_id} with {new_value} at timestamp {self.tick}'
            )

        txn = self.txns[txn_id]
        txn.current_instruction = instr

        # go acquire exclusive locks for the item
        exclusive_locks = self.site_manager.acquire_exclusive_lock(
            txn_id, item_id)
        if len(exclusive_locks) > 0:

            # for each exclusive lock returned, record down the sites
            locked_sites = []
            for l in exclusive_locks:
                if l not in txn.locks_holding:
                    txn.locks_holding.append(l)
                locked_sites.append(l.site_id)

            # add this new uncommitted value to transaction's uncommitted values dictionary
            txn.uncommit_values[item_id] = new_value

            # add this operation to the transaction's cache
            txn.cache[self.tick] = [
                "write", (item_id, new_value, locked_sites)
            ]
        else:
            txn.status = "blocked"

    def fail(self, site_id):
        """ Tells the Site_manager to fail a site

        Parameters:
            site_id (int): ID of site to be failed
        """

        self.site_manager.fail(site_id, self.tick)
        if self.debug: print(f'Site {site_id} failed at timestamp {self.tick}')

    def recover(self, site_id):
        """ Tells the Site_manager to recover a site
        Add all the transactions in txns_waiting_list back to the txns_ready_list to see if any can proceed

        Parameters:
            site_id (int): ID of site to be recovered
        """

        self.site_manager.recover(site_id, self.tick)
        if self.debug:
            print(f'Site {site_id} recover at timestamp {self.tick}')

        self.site_manager.txns_ready_list = self.site_manager.txns_ready_list + self.site_manager.txns_waiting_list
        self.site_manager.txns_waiting_list = []

    def dump(self):
        """ Tells the Site_manager to dump all the variables and their values
        """

        if self.debug: print("dumping stuff", "at timestamp", self.tick)
        self.site_manager.dump()

    def end(self, instr, txn_id):
        """ Ends transaction. 
        Asks site_manager if the transaction should commit based on the timestamp of operations vs site_failures
        If okay, asks site_manager to commit
        Else, ask site_manager to abort
        In the end, add all the transactions in txns_waiting_list back to the txns_ready_list to see if any can proceed

        Parameters:
            instr (string): The original instruction string.
            txn_id (int): ID of transaction to be ended
        """

        if self.debug:
            print("Ending Transaction", txn_id, "at timestamp", self.tick)
        txn = self.txns[txn_id]
        txn.current_instruction = instr

        # Check if transaction has already been committed or aborted
        if txn.status == "committed" or txn.status == "aborted":
            print(f'Transaction {txn_id} has already been {txn.status}')

        # Check if transaction is blocked or not
        elif txn.status == "blocked":
            print(f'Cannot end because Transaction {txn_id} is being blocked.')

        elif txn.transaction_type == "read_write":
            if self.debug:
                print(f'Transaction {txn_id} \'s operations were:')
                print(txn.cache)

            # Ask site manager to check if transaction can commit
            if self.site_manager.check_commit(txn):
                print(f'Transaction {txn_id} commits.')

                # ask site manager to commit
                self.site_manager.commit(txn)
                txn.status = "committed"
            else:
                print(f'Transaction {txn_id} aborts.')

                # ask site manager to abort
                self.site_manager.abort(txn)
                txn.status = "aborted"
        else:
            print(f'Transaction {txn_id} commits')

            # ask site manager to commit a Read Only transaction
            self.site_manager.commitRO(txn)
            txn.status = "committed"

        # add all the transactions in txns_waiting_list back to the txns_ready_list to see if any can proceed in the next tick
        self.site_manager.txns_ready_list = self.site_manager.txns_ready_list + self.site_manager.txns_waiting_list
        self.site_manager.txns_waiting_list = []

    def run_ready_transactions(self):
        # Run transactions that are ready

        while len(self.site_manager.txns_ready_list) > 0:
            if self.debug:
                print("Transactions ready to run again --->",
                      self.site_manager.txns_ready_list)

            # pop the transaction out
            ready_txn_id = self.site_manager.txns_ready_list.pop(0)
            print(f'Resume Transaction {ready_txn_id}')
            if self.debug:
                print(
                    f'Transaction {ready_txn_id} goes from {self.txns[ready_txn_id].status} to running'
                )

            self.txns[ready_txn_id].status = "running"
            self.read_instruction(self.txns[ready_txn_id].current_instruction)
            print()

    def find_cycle(self):
        # Tell site_manager to detect cycle, if cycle found, kill the youngest cycle

        cycles = self.site_manager.find_cycle()
        if len(cycles) > 0:

            for cycle in cycles:
                print(f'Cycle detected in Transactions {cycle}')
                self.kill_youngest(cycle)

    def kill_youngest(self, cycle):
        """ Finds the youngest transaction in the cycle and tells the site_manager to kill that transaction
        
        Parameters:
           cycle (list(int)): list of transaction IDs that are in a cycle
        """

        cycle.sort(key=lambda txn_id: self.txns[txn_id].timestamp,
                   reverse=True)
        youngest_txn = self.txns[cycle[0]]
        print(f'Killing the youngest Transaction {cycle[0]}')
        youngest_txn.status = "aborted"
        self.site_manager.kill(youngest_txn)
        print()

    def query_state(self):
        # prints info of all the transactions in Transaction Manager
        print("Transaction State:")
        for _, txn in self.txns.items():
            print(f'T{txn.id}: {txn.status}')
