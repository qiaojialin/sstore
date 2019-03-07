
# SStore

This is a simplified version of SummaryStore: https://github.com/ayvee/summarystore

## Basic Functions

### Write a stream: src/main/java/InsertTest

* Put all summary windows into RocksDB
* Read,merge,put,update summary windows in RocksDB
* Serialize the stream metadata into RocksDB

### Query a stream: src/main/java/QueryTest

* DeSerialize the stream metadata from RocksDB
* Get all summary windows overlapped in RocksDB
* Estimate the error and return result

## Modification

* Remove SummaryStore class
* Remove landmark window
* Remove avl index for summary window
* Remove memory backing store for test
* Only retain the count operator
* ...