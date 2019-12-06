# Replicated Concurrency Control and Recovery Documentation

## AUTHORS

Ian Lam (iil209), Yu Ting Chiu (ytc338)

## PROGRAMMING LANGUAGE

Python 3.6

## PROJECT DESCRIPTION

This project implements a distributed database system of 10 sites and 20 variables. It takes in an input text file and runs the operations in the file line by line. For read operations, it will output onto the screen the data value of the desired item. For end operations, it will output rather a transaction commits or aborts. If a transaction cannot acquire a lock or a snapshot of database or is killed due to deadlock, the program will output that as well.

## TO RUN THE PROGRAM:

    `python3 main.py [input_file]`

## ALGORITHMS USED

### Available Copies Algorithm

- Returns the first available copy. If the item is replicated, the Site Manager will start looking from Site 1. If the item is not available in any site, the transaction will wait.
- At commit time, the Site Manager determines whether a transaction commits based on the timestamp of each of its operations and the timestamp of the last failure from the sites the transaction has touched. If there is a site that has failed, the transaction will abort, otherwise, it will commit.
- If a transaction is blocked because it cannot acquire lock or a site has failed, it will resume once the item is available or the site is recovered.

### Strict Two Phase Locking

- The Site Manager will determine whether a transaction can acquire a lock. All locks are released at the end of a transaction.
- For shared locks, a transaction can obtain lock for an item if no other transaction is waiting for the item.
- For exclusive locks, a transaction can obtain lock if no other transaction is reading or writing the item.

### Deadlock Detection

- The Site Manager looks for deadlock every time a new line is read from the input file. It uses the Tarjan’s Algorithm to find strongly connected components in the wait for graph
- The Site Manager returns any cycle found to the Transaction Manager, and the Transaction Manager kills the youngest.

### Mulitversion Concurrency Control

- Read only transaction acquires a snapshot of the database when the transaction begins. All values that have been committed at that time point will be recorded by the transaction. If an item is not available, the transaction will wait and acquire the snapshot later when the site recovers.

### Site Failure and Site Recovery

- When a site fails, its lock table is wiped out.
- When a site recovers, all the unreplicated items are immediately available for reads and writes. Replicated items need to wait for a committed write.

## COMPONENTS

1. Main: Entry point of the program. Uses sys.argv[1] to get the input file name. Sends instructions to Transaction Manager by going though the input file line by line.

2. Transaction Manager: Transaction Manager is responsible for parsing inputs and delegating any operation that requires site interaction to the site manager.

3. Transaction: A unit of work that has a number of operations performed within the database.

4. Site Manager: Site Manager is responsible for acquiring locks, releasing locks, failing sites, recovering sites, making changes to data items' values, figuring out where the deadlocks are, deciding whether to commit or abort a transaction. It acts as middleman for the Transaction Manager and the 10 sites.

5. Site: Site is where the data items are stored. It receives directions from the Site Manager regarding failing and recovering of a site. Each site has a data table and a lock table.

6. Variable: Variable is the data item.

7. Lock: Lock locks a data item. A transaction needs to acquire a lock before it can access the data item.
