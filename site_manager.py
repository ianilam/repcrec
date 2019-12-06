'''
Created on Nov 27, 2019

@author: Ian Lam, Yu Ting Chiu
'''

from one_site import Site
from lock import Lock
from collections import OrderedDict


class Site_Manager(object):
    """
    Site_Manager is responsible for acquiring locks, releasing locks, failing
    sites, recovering sites, making changes to data items' values, figuring out
    where the deadlocks are, deciding whether to commit or abort a transaction
    
    Attributes:
        sites (list(Site Object)): List of Site Objects
        data_site_map (dict (int: list(int) ): Mapping of the data item and the sites that contains it
        item_txns_waiting_map (dict (int: OrderedDict) ): Mapping of the data item and the transactions waiting for item
        txns_waiting_list (list(int)): List of transaction_ids that are unable to obtain item due to site failure or items not committed after site failure
        txns_ready_list (list(int)): List of transaction_ids that are ready to be run because the data item they are waiting for is available
        txns_ended_list (list(int, str)): List of transaction_ids that have ended and their respective status
    """
    def __init__(self):
        self.sites = []
        self.data_site_map = {}
        self.item_txns_waiting_map = {}
        self.txns_waiting_list = []
        self.txns_ready_list = []
        self.txns_ended_list = []

        self.debug = False

        # initialize the 10 sites
        for i in range(10):
            s = Site(i + 1)
            self.sites.append(s)

        # initialize the data_site_map
        for i in range(1, 21):
            if i % 2 == 0:
                # even items replicated at all sites
                self.data_site_map[i] = self.sites
            else:
                # odd items only at specific sites
                self.data_site_map[i] = [self.sites[i % 10]]

        # initialize the item_txns_waiting_map with all 20 item_ids
        for i in range(1, 21):
            self.item_txns_waiting_map[i] = OrderedDict()

    def fail(self, siteID, timestamp):
        """Fails a site with siteID
    
        Parameters:
            siteID (int): ID of site to be failed
            timestamp (int): time of this action
        """

        self.sites[int(siteID) - 1].fail(timestamp)

    def recover(self, siteID, timestamp):
        """Recovers a site with siteID
    
        Parameters:
            siteID (int): ID of site to be recover
            timestamp (int): time of this action
        """

        self.sites[int(siteID) - 1].recover(timestamp)

    def dump(self):
        # Dump all the sites and their data variable and values

        for s in self.sites:
            s.dump_site()

    def acquire_share_lock(self, txn_id, item_id):
        """Acquire shared locks for Transaction txn_id for data item item_id
        
        Parameters:
            txn_id (int): Transaction ID
            item_id (int): ID of item wanted
            
        Return:
            if successful: A Lock object of type 'SL' 
            else: None
        
        Side Effect:
            If lock not acquired due to another transaction holding, the txn_id is added to the item_txns_waiting_map.
            If lock not acquired due to site failures, the txn_id is added to txns_waiting_list
        """

        locations = self.data_site_map[item_id]
        for location in locations:
            # check whether the site is normal
            if location.status == "normal":

                # check whether the item is available for read in this site
                if item_id in location.readable_variables:

                    if self.debug:
                        print(
                            f'Transaction {txn_id} attempts to acquire read lock for x{item_id} at Site {location.site_id}'
                        )

                    # check the lock table of that item
                    if location.lock_table[item_id] == None:

                        # nothing holding the item, make a new lock for the item
                        location.lock_table[item_id] = Lock(
                            "SL", txn_id, location.site_id,
                            location.data_table[item_id])
                        if self.debug: print('Received shared lock')
                        return location.lock_table[item_id]

                    elif location.lock_table[item_id].lock_type == "SL" and len(
                            self.item_txns_waiting_map[item_id]) == 0:
                        # if the item already has a SL from another transaction, and there is no other transactions waiting for item, then share lock
                        if self.debug:
                            print(
                                f'Received shared lock, sharing with Transactions {location.lock_table[item_id].txn_holding}'
                            )
                        location.lock_table[item_id].txn_holding.append(txn_id)
                        return location.lock_table[item_id]

                    elif location.lock_table[
                            item_id].lock_type == "XL" and location.lock_table[
                                item_id].txn_holding[0] == txn_id:
                        # the transaction already holding an Exclusive lock on the item, just return the current lock
                        if self.debug:
                            print(
                                f'Already holding the exclusive lock on this item'
                            )
                        return location.lock_table[item_id]

                    else:
                        # item locked by exclusive lock from other transactions
                        txn_blocking = location.lock_table[item_id].txn_holding
                        print(
                            f'Fail to acquire shared lock because Transaction {txn_blocking} currently locked x{item_id}'
                        )
                        transactions_ahead = list(
                            self.item_txns_waiting_map[item_id].keys())
                        self.item_txns_waiting_map[item_id][txn_id] = set(
                            transactions_ahead + txn_blocking)
                        print(
                            f'Need to wait for Transactions {set(transactions_ahead + txn_blocking)}'
                        )
                        return None

        # All sites not available
        print(
            f'Not able to acquire shared lock for x{item_id} either due to site failure or the data item not updated.'
        )
        self.txns_waiting_list.append(txn_id)
        return None

    def acquire_exclusive_lock(self, txn_id, item_id):
        """Acquire exclusive locks for Transaction txn_id for data item item_id
        
        Parameters:
            txn_id (int): Transaction ID
            item_id (int): ID of item wanted
            
        Return:
            if successful: List of Lock objects of type 'XL' 
            else: Empty list
        
        Side Effect:
            If transaction already holds SL for the item and no other transaction is waiting for item, the SL is promoted to XL
            If lock not acquired due to another transaction holding, the txn_id is added to the item_txns_waiting_map.
        """

        acquired_exclusive_locks = []
        locations = self.data_site_map[item_id]

        for location in locations:

            # check whether the site is normal
            if location.status == "normal":

                # check the lock table of that item
                if self.debug:
                    print(
                        f'Transaction {txn_id} attempts to acquire exclusive lock for x{item_id} at Site {location.site_id}'
                    )

                if location.lock_table[item_id] == None:
                    # nothing holding the item, make a new lock for the item
                    location.lock_table[item_id] = Lock(
                        "XL", txn_id, location.site_id,
                        location.data_table[item_id])
                    if self.debug: print(f'Received exclusive lock')
                    acquired_exclusive_locks.append(
                        location.lock_table[item_id])

                elif location.lock_table[item_id].lock_type == "SL" and len(
                        self.item_txns_waiting_map[item_id]
                ) == 0 and location.lock_table[item_id].txn_holding[
                        0] == txn_id and len(
                            location.lock_table[item_id].txn_holding) == 1:
                    # if item is locked with SL, and no other transaction waiting for item, and this current transaction is the one holding the SL, and no other transaction is reading the item
                    if self.debug: print(f'Promote lock from SL to XL')
                    location.lock_table[item_id].lock_type = "XL"
                    acquired_exclusive_locks.append(
                        location.lock_table[item_id])

                elif location.lock_table[
                        item_id].lock_type == "XL" and location.lock_table[
                            item_id].txn_holding[0] == txn_id:
                    # exclusive lock already held by transaction
                    if self.debug:
                        print(f'Transaction already holds XL on this item!')
                    acquired_exclusive_locks.append(
                        location.lock_table[item_id])

                else:
                    # someone is reading or writing it
                    txn_blocking = location.lock_table[item_id].txn_holding
                    print(
                        f'Fail to acquire exclusive lock because Transactions {txn_blocking} currently locked x{item_id}'
                    )
                    transactions_ahead = list(
                        self.item_txns_waiting_map[item_id].keys())
                    self.item_txns_waiting_map[item_id][txn_id] = set(
                        transactions_ahead + txn_blocking)
                    print(
                        f'Need to wait for Transactions {set(transactions_ahead + txn_blocking)}'
                    )
                    return []

        return acquired_exclusive_locks

    def check_commit(self, txn):
        """Check whether the transaction should commit based on the timestamp of its operations and the timestamps of touched sites
        
        Parameters:
            txn (Transaction Object): Transaction to check for
            
        Return:
            boolean for whether to commit or not
        """

        if txn.transaction_type == "read_write":
            for ts, action in txn.cache.items():
                _, _, touched_sites = action[1]
                for s in touched_sites:
                    if self.sites[s - 1].last_fail_timestamp > ts:
                        print(
                            f'Site {s} has failed after Transaction {txn.id} obtained lock'
                        )
                        return False

            return True

    def commit(self, txn):
        """ Commits transaction by actually writing to sites with new value and release all the locks it holds
        
        Parameters:
            txn (Transaction Object): Transaction to commit
            
        Side Effects:
            Sites are updated with the new written values
        """

        # first write the items
        for _, action in txn.cache.items():
            if action[0] == "write":
                item_id, new_val, destinations = action[1]
                self.write(item_id, new_val, destinations)

        # second release all locks hold by that transaction
        self.release_locks(txn)

        self.txns_ended_list.append((txn.id, "committed"))

    def commitRO(self, txn):
        self.txns_ended_list.append((txn.id, "committed"))

    def abort(self, txn):
        """ Aborts transaction and releases all the locks it holds
        
        Parameters:
            txn (Transaction Object): Transaction to abort  
        """

        # just release all locks since the transaction will abort
        self.release_locks(txn)

        self.txns_ended_list.append((txn.id, "aborted"))

    def kill(self, txn):
        """ Kills transaction, removes transaction in the item_txns_waiting_map, releases all the locks it holds
        
        Parameters:
            txn (Transaction Object): Transaction to abort  
        """
        # get rid of transaction in the item_waiting map
        for _, transaction_waiting in self.item_txns_waiting_map.items():
            if txn.id in transaction_waiting:
                del transaction_waiting[txn.id]

        # release the locks
        self.release_locks(txn)

        # delete txn_id from the items_txns_waiting map
        for item_id, transaction_waiting in self.item_txns_waiting_map.items():

            to_delete = False
            for transaction_after, transaction_before in transaction_waiting.items(
            ):
                if txn.id in transaction_before:
                    transaction_before.remove(txn.id)
                if len(transaction_waiting) == 1 and len(
                        transaction_before) == 0:
                    # the transction_after doesn't have anymore transaction to wait for
                    self.txns_ready_list.append(transaction_after)
                    to_delete = True

            if to_delete:
                self.item_txns_waiting_map[item_id] = OrderedDict()

        self.txns_ended_list.append((txn.id, "killed"))
        if self.debug: print("Waiting:", self.txns_waiting_list)

    def release_locks(self, txn):
        """ Releases all locks from the transaction, updates the lock_table of where the lock is from
        
        Parameters:
            txn (Transaction Object): Transaction to release locks from
            
        Side Effect:
            Add new transaction to txns_ready_list if the transaction can use the free data item
        """

        for l in txn.locks_holding:
            if l.lock_type == "SL" and len(l.txn_holding) > 1:
                l.txn_holding.remove(txn.id)

            else:
                location = self.sites[l.site_id - 1]
                location.lock_table[l.item_locked.id] = None

                # item becomes available
                if self.debug:
                    print(
                        f'Released {l.lock_type} for x{l.item_locked.id}. x{l.item_locked.id} is now available at Site {l.site_id}.'
                    )

            # remove the transaction in the item_txns_waiting_map
            for _, transaction_before in self.item_txns_waiting_map[
                    l.item_locked.id].items():
                if txn.id in transaction_before:
                    transaction_before.remove(txn.id)

            # check if any new transaction can proceed
            for transaction_waiting, transaction_before in self.item_txns_waiting_map[
                    l.item_locked.id].items():
                if len(transaction_before) == 0:
                    self.txns_ready_list.append(transaction_waiting)
                    del self.item_txns_waiting_map[
                        l.item_locked.id][transaction_waiting]
                    if self.debug:
                        print(
                            f'Transaction {transaction_waiting} can use x{l.item_locked.id} now'
                        )
                elif len(transaction_before) == 1:
                    if list(transaction_before)[0] == transaction_waiting:
                        self.txns_ready_list.append(transaction_waiting)
                        del self.item_txns_waiting_map[
                            l.item_locked.id][transaction_waiting]
                        if self.debug:
                            print(
                                f'Transaction {transaction_waiting} can use x{l.item_locked.id} now'
                            )
                break

        txn.locks_holding = []
        if self.debug: print(f'All locks released for Transaction {txn.id}')

    def write(self, item_id, new_val, destinations):
        """ Writes the new value to the database
        
        Parameters:
            item_id (int): ID of item to be written to
            new_val (int): new value of the item
            destinations (list(int)): list of site IDs of where the new value should be written to
        """

        for i in destinations:
            destination = self.sites[i - 1]
            destination.data_table[item_id].value = new_val

            # this is for updating the readable_variables
            destination.readable_variables.add(item_id)

        print(
            f'Commits new value {new_val} to x{item_id} in Sites {destinations}'
        )

    def acquire_snapshot(self, txn_id):
        """ Acquires snapshot of database with each data item and its value
        Parameters:
            txn_id: Transaction ID that's acquiring snapshot
            
        Return:
            dict(int: int) of snapshot. {item_id: item value}
        """
        snapshot = {}

        for i in range(1, 21):
            locations = self.data_site_map[i]
            snapshot[i] = None
            for location in locations:
                if location.status == "normal" and i in location.readable_variables:
                    snapshot[i] = location.data_table[i].value
                    break

            if snapshot[i] == None:
                print(
                    f'Transaction {txn_id} cannot acquire x{i} for the snapshot because it\'s not available in any site.'
                )
                self.txns_waiting_list.append(txn_id)
                return None

        return snapshot

    def find_cycle(self):
        """ Check for deadlock. Use Tarjan’s Algorithm to find Strongly Connected Components (cycles)
        
        Return:
            list( list(int) ): List of cycles. Each cycle is a list of Transaction IDs
        """

        # turn item_txns_waiting_map into a graph
        graph = self.create_graph()

        disc = {}
        for vertex in list(graph.keys()):
            disc[vertex] = -1

        low = {}
        for vertex in list(graph.keys()):
            low[vertex] = -1

        stackMember = {}
        for vertex in list(graph.keys()):
            stackMember[vertex] = False

        st = []
        time = 0

        ans = []

        def find_cycle_helper(u, low, disc, stackMember, st, time, graph):
            disc[u] = time
            low[u] = time
            time += 1
            stackMember[u] = True
            st.append(u)

            for v in graph[u]:
                if disc[v] == -1:
                    find_cycle_helper(v, low, disc, stackMember, st, time,
                                      graph)
                    low[u] = min(low[u], low[v])
                elif stackMember[v] == True:
                    low[u] = min(low[u], disc[v])

            w = -1
            if low[u] == disc[u]:
                cycle = []
                while w != u:
                    w = st.pop()
                    cycle.append(w)
                    stackMember[w] = False

                if len(cycle) > 1:
                    ans.append(cycle)

        for i in list(graph.keys()):
            if disc[i] == -1:
                find_cycle_helper(i, low, disc, stackMember, st, time, graph)

        return (ans)

    def create_graph(self):
        """ Creats a wait-for graph from item_txns_waiting_map
        
        Return:
            graph (dict(int, set()) ): transaction and the transactions it's waiting for 
        """

        graph = {}

        vertices_seen = set()

        for _, txns_waiting in self.item_txns_waiting_map.items():
            for txns_second, txns_first in txns_waiting.items():
                vertices_seen = vertices_seen.union(txns_first)
                if txns_second in graph:
                    graph[txns_second].union(txns_first)
                else:
                    graph[txns_second] = txns_first

        for vertex in list(vertices_seen):
            if vertex not in graph:
                graph[vertex] = set()

        for txns_second, txns_first in graph.items():
            if txns_second in txns_first:
                txns_first.remove(txns_second)

        return graph
