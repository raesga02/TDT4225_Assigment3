from pprint import pprint 
from time import sleep
from DbConnector import DbConnector
from Queries import QueriesLibrary
from tabulate import tabulate

import os
import csv
import sys
import argparse

class Geolife:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.query_library = QueriesLibrary(self.db)

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_documents(self, collection_name):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         
def argument_parser():
    parser = argparse.ArgumentParser(description='Geolife database manager')
    parser.add_argument('-i', '--initialize', action='store_true', help='Initialize data into database')
    parser.add_argument('-f', '--fetch_all', action='store_true', help='Fetch data from database')
    parser.add_argument('-d', '--drop_all', action='store_true', help='Drop tables from database')
    parser.add_argument('-q', '--query', type=int, help='Query number to execute')
    return parser.parse_args()

def main():
    program = None
    try:
        program = Geolife()
        args = argument_parser()

        if args.initialize:
            pass
            # program.create_tables()
            # program.insert_data()

        if args.fetch_all:
            pass
            # _ = program.fetch_data(table_name="User")
            # _ = program.fetch_data(table_name="Activity")
            # _ = program.fetch_data_batch(table_name="TrackPoint")

        if args.query:
            program.query_library.queries[args.query]()

        if args.drop_all:
            pass
            # program.drop_table(table_name="TrackPoint")
            # program.drop_table(table_name="Activity")
            # program.drop_table(table_name="User")
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
