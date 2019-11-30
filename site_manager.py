'''
Created on Nov 27, 2019

@author: ian
'''
from site import Site
from lock import Lock
from collections import OrderedDict

class Site_Manager (object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self.sites = []
        self.data_site_map = {}
        self.item_txns_waiting_list = {} # item: (Transaction needing, Transacting holding the lock)
        self.txns_waiting_list = [] # transactions waiting because it could not get item due to site failure or the item not yet updated
        self.txns_ready_list = []
        self.txns_ended_list = []
        
        # initialize the sites
        for i in range(10):
            s = Site(i+1)
            self.sites.append(s)
        
        # initialize the data_to_site_map
        for i in range(1, 21):
            if i % 2 == 0:
                # even items replicated at all sites
                self.data_site_map[i] = self.sites
            else:
                # odd items only at specific sites
                self.data_site_map[i] = [self.sites[i%10]]
        
        # initialize the txns
        for i in range(1, 21):
            self.item_txns_waiting_list[i] = OrderedDict()
            
    
    def fail(self, siteID, timestamp):
        self.sites[int(siteID)-1].fail(timestamp)

            
    
    def recover(self, siteID, timestamp):
        self.sites[int(siteID)-1].recover(timestamp)

    def dump(self):
        for s in self.sites:
            s.dump_site()
    
    # either acquire ONE lock or None
    # parameters: txn_id = transaction id, item_id = data item id that needs to be read
    # returns a Share Lock if able to acquire else None
    def acquire_share_lock(self, txn_id, item_id):
        locations = self.data_site_map[item_id]
        for location in locations:
        # check whether the site is normal
            if location.status == "normal":
                if item_id in location.readable_variables:
                    # check the lock table of that item
                    print(f'Transaction {txn_id} attempts to acquire read lock for x{item_id} at Site {location.site_id}')
                    if location.lock_table[item_id] == None:
                        # nothing holding the item
                        # make a new lock for the item
                        location.lock_table[item_id] = Lock("SL", txn_id, location.site_id, location.data_table[item_id])
                        print('Received shared lock')
                        return location.lock_table[item_id]
                    elif location.lock_table[item_id].lock_type == "SL" and len(self.item_txns_waiting_list[item_id]) == 0:
                        # share lock holding the item
                        # append current transaction to the list of txns with this lock
                        print(f'Other transactions currently holding a shared lock on the item are {location.lock_table[item_id].txn_holding}')
                        print(f'Received shared lock, sharing with {location.lock_table[item_id].txn_holding}')
                        location.lock_table[item_id].txn_holding.append(txn_id)
                        return location.lock_table[item_id]
                            
                    # exclusive lock
                    else:
                        print(f'Fail to acquire lock')
                        txn_blocking = location.lock_table[item_id].txn_holding
                        print(f'Transactions {txn_blocking} currently locked {item_id}')
                        transactions_ahead = list(self.item_txns_waiting_list[item_id].keys())
                        self.item_txns_waiting_list[item_id][txn_id] = set(transactions_ahead + txn_blocking)
                        print(f'Need to wait for Transactions{set(transactions_ahead + txn_blocking)}')
#                         self.txns_blocked_list.append(txn_id)
                        return None
        
        print(f'Not able to acquire lock for x{item_id} due to site failure')
        self.txns_waiting_list.append(txn_id)
        # then you have to wait
        return None
    
    def read(self, site_id, item_id):
        location = self.sites[site_id-1]
        return location.data_table[item_id]
    
    def acquire_exclusive_lock(self, txn_id, item_id):
        acquired_exclusive_locks = []
        locations = self.data_site_map[item_id]
        for location in locations:
        # check whether the site is normal
            if location.status == "normal":
                # check the lock table of that item
                print(f'Transaction {txn_id} attempts to acquire exclusive lock for x{item_id} at Site {location.site_id}')
                if location.lock_table[item_id] == None:
                    # nothing holding the item
                    # make a new lock for the item
                    location.lock_table[item_id] = Lock("XL", txn_id, location.site_id, location.data_table[item_id])
                    print(f'Received exclusive lock')
                    # append the newly acquired lock to the results
                    acquired_exclusive_locks.append(location.lock_table[item_id])
                elif location.lock_table[item_id].lock_type == "SL" and len(self.item_txns_waiting_list[item_id]) == 0 and location.lock_table[item_id].txn_holding[0] == txn_id:
                    print(f'Promote lock from SL to XL')
                    location.lock_table[item_id].lock_type = "XL"
                    acquired_exclusive_locks.append(location.lock_table[item_id])
                else:
                    # someone is reading it
                    print(f'Fail to acquire lock because Transaction has other locks on it')
                    txn_blocking = location.lock_table[item_id].txn_holding
                    print(f'Transactions {txn_blocking} currently locked {item_id}')
                    transactions_ahead = list(self.item_txns_waiting_list[item_id].keys())
                    self.item_txns_waiting_list[item_id][txn_id] = set(transactions_ahead + txn_blocking)
                    print(f'Need to wait for Transactions {set(transactions_ahead + txn_blocking)} first')
#                     self.txns_blocked_list.append(txn_id)
                    return []
                
        return acquired_exclusive_locks
    
    def check_commit(self, cache):
        for ts, action in cache.items():
            _, _, touched_sites = action[1]
            for s in touched_sites:
                if self.sites[s-1].last_fail_timestamp > ts:
                    print(f'Site {s} has failed after transaction obtained lock')
                    return False
        
        return True
    
    def commit(self, txn):
        # to commit, first write the items
        for _, action in txn.cache.items():
            if action[0] == "write":
                item_id, new_val, destinations = action[1]
                self.write(item_id, new_val, destinations)
        print(f'Transaction {txn.id} finished putting committed values in database')
        # second release all locks hold by that transaction
        self.release_locks(txn)
        self.txns_ended_list.append((txn.id, "committed"))
        
    def abort(self, txn):
        # just release all locks since the transaction will abort
        self.release_locks(txn)
        self.txns_ended_list.append((txn.id, "aborted"))
        
    def kill(self, txn):
        
        # get rid of yourself in the item_waiting list
        for _, transaction_waiting in self.item_txns_waiting_list.items():
            if txn.id in transaction_waiting:
                del transaction_waiting[txn.id]
        
        # release the locks
        self.release_locks(txn)
        self.txns_ended_list.append((txn.id, "killed"))
        
    def release_locks(self, txn):
        
        for l in txn.locks_holding:
#             l.getInfo()
            # remove that lock from the lock table at that particular site
#             print(l.txn_holding)
#             print(f'Transactions waiting for {l.item_locked.id}: {self.item_txns_waiting_list[l.item_locked.id]}')
            if l.lock_type == "SL" and len(l.txn_holding) > 1:
#                 print(l.txn_holding)
                l.txn_holding.remove(txn.id)
                
            else:
                location = self.sites[l.site_id-1]
                location.lock_table[l.item_locked.id] = None
                
                # item becomes available
                print(f'Released {l.lock_type} for x{l.item_locked.id}. x{l.item_locked.id} is now available at Site {l.site_id}.')
            
            # remove the transaction in the wait for graph    
            for _, v in self.item_txns_waiting_list[l.item_locked.id].items():
                if txn.id in v:
                    v.remove(txn.id)
                    
            # check if the txn can proceed
            for k, v in self.item_txns_waiting_list[l.item_locked.id].items():
                if len(v) == 0:
                    self.txns_ready_list.append(k)
                    del self.item_txns_waiting_list[l.item_locked.id][k]
                    print(f'Transaction {k} can use x{l.item_locked.id} now')
                elif len(v) == 1:
                    if list(v)[0] == k:
                        self.txns_ready_list.append(k)
                        del self.item_txns_waiting_list[l.item_locked.id][k]
                        print(f'Transaction {k} can use x{l.item_locked.id} now')
                break
                
#         print(f'After removal of transaction: {self.item_txns_waiting_list[l.item_locked.id]}')
#                 print("txns waiting due to site failure:", self.txns_waiting_list)
#                 print("txns ready:", self.txns_ready_list)
#             print(f'Transactions waiting for {l.item_locked.id}: {self.item_txns_waiting_list[l.item_locked.id]}')
            
        txn.locks_holding = []
        print(f'All locks released for Transaction {txn.id}')
#         print(f'Waiting for items: {self.item_txns_waiting_list}')
    
    
    def write(self, item_id, new_val, destinations):
        for i in destinations:
            destination = self.sites[i-1]
            destination.data_table[item_id].value = new_val
            
            # this is for updating the readable_variables
            destination.readable_variables.add(item_id)
        
        print(f'Writes new value {new_val} to x{item_id} in Sites {destinations}')
    
    
    def acquire_snapshot(self):
        snapshot = {}
         
        for i in range(1, 21):
            # figure out which site the data_item is at:
            locations = self.data_site_map[i]
            snapshot[i] = None
            for location in locations:
                if location.status == "normal":
                    snapshot[i] = location.data_table[i].value
                    break
                
        return snapshot
    
#     def show_which_txns_waiting_for_which_item(self):
#         print(self.item_txns_waiting_list)
#         self.find_cycle()
        
    def find_cycle(self):
        
        # turn item_txns_waiting_list into a graph
        graph = self.create_graph()
        print("Transaction Waiting Graph:", graph)
        
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
                    find_cycle_helper(
                        v, low, disc, stackMember, st, time, graph)
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
    
        return(ans)
        
    def create_graph(self):
        
        graph = {}
#         
#         all_vertices = set()
#         
#         for _, txns_waiting in self.item_txns_waiting_list.items():
#             for txns_second, txns_first in txns_waiting.items():
#                 all_vertices.add(txns_second)
#                 all_vertices.union(txns_first)
#         
#         for vertex in list(all_vertices):
#             graph[vertex] = set()
        
        vertices_seen = set()
        
        for _, txns_waiting in self.item_txns_waiting_list.items():
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
                
#         print(graph)
        return graph
        