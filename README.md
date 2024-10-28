# Local Database implementation

## This is a work in progress branch, 

> It is a package level database implementation, Non SQL data manager, like in good old times when we used dBase.

Features:
- Insert
- Locate
- Seek
- Fetch
- Next
- Prev
... and what is coming

Indexes:
- Binary Tree | Only for search (not yet finished)
- BTree (balanced tree) | Search and order
- HashMap | Only for search | not yet implemented


(some benchmark, Table with 3 indexes, 100 million rows. Seek time from BTree 2 millisecond, insert (updating 3 indexes) 3 millisecond, not bad for an experimental code)