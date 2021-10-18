### Automation API in regional servers with Airflow


#### Running the API
> uvicorn main:app --host 0.0.0.0 --port 3333 --forwarded-allow-ips "*" --reload


### TODO
- [x] check_regional_tar - check consistency of sent and recieved .tar
- [x] check_regional_data - check consistency of untarred sent files with sent files
- [x] faiss_create_new_index - update backuped faiss index with vectors and ids from pickle file 
      (sent from central server) (insert_with_ids) using blocks: block_1.index, block_2.index, etc.*
- [x] update_temporary_tables - update postgres tables (3 tables)
- [x] Create docker and docker-compose
- [ ] Check whether some vectors where inserted into faiss correctly by doing search
- [ ] Send insert/delete statistics with IP address of the server to Central machine

\* block_1.index is the very first database, block_2 is the first delta, block_3, block_n...

will be the second and so on deltas from previous databases.
