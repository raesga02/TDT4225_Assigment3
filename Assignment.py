import csv
import os
import argparse
from pprint import pprint 
from DbConnector import DbConnector
from Queries import QueriesLibrary

class Geolife:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.query_library = QueriesLibrary(self.db)

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_all_documents(self):

        #Get the collections
        user_collection = self.db["Users"]
        activities_collection = self.db["Activities"]
        trackpoints_collection = self.db["Trackpoints"]

        #Get the users that have labels
        labels_list = open("./dataset/labeled_ids.txt", "r").read().split("\n")
        labels_list.remove("")
        labels_list = [int(label) for label in labels_list]
        labels_dict = dict.fromkeys(labels_list,True)

        #Variables to keep track of activities ids 
        activities_id = dict()
        activities_counter = 1
        trackpoint_counter = 1
        
        #Control variables
        skip_first = True
        id = -1

        #To save documents before adding them to the collections
        track_values = list()
        activities_docs = list()
        user_docs = list()
        
        #Iterate the database
        for (root,dirs,files) in os.walk('./dataset/Data', topdown=True):
            
            #Skip the first iteration that shows all the data directories
            if(skip_first):
                skip_first = False 
                continue
            
            #If we are in the users folder
            if("Trajectory" in dirs):
                
                #If there is a good batch of data to insert
                if (len(user_docs) > 10):
                    #Insert data if there is trackpoints to insert
                    print(f"Inserting {[user['_id'] for user in user_docs] }")
                    trackpoints_collection.insert_many(track_values)
                    activities_collection.insert_many(activities_docs)
                    user_collection.insert_many(user_docs)  

                    #Clear lists
                    track_values = list()
                    activities_docs = list()
                    user_docs = list()
                    

                #Get the user id
                id = int(root.split("dataset/Data/")[1])
                
                #If the user is not in the dictionary add it as false
                if(id not in labels_dict):
                    labels_dict[id] = False
                
                #Create the user, no activities yet. 
                user = {
                    "_id": id,
                    "has_label": labels_dict[id],
                    "activities": []
                }
                user_docs.append(user)

                #Set up the dictionary of activities for this id
                activities_id = {}
                #If the user has activity data
                if(labels_dict[id]):
                    #Open the file
                    with open(root + "/labels.txt", 'r') as file:
                        #Skip header
                        next(file)
                        #Read data
                        csvreader = csv.reader(file, delimiter='\t')
                        for row in csvreader:
                            #Keep track of data in memory
                            activities_id[row[0].replace("/", "-") + " - " + row[1].replace("/", "-")] = row[2]
            
            else:
                #For every file
                for file_name in files:
                    #Open the file
                    with open(root + "/" + file_name, 'r') as file:
                        #Get a list of all the lines of the file
                        content = file.readlines()
                        #If the size of the file is too big, dont add it
                        if(len(content) - 6 > 2500):
                            continue
                        
                        #Get first and last timestamp
                        first_line = content[6].strip("\n").split(",")
                        last_line = content[-1].strip("\n").split(",")
                        track_start = first_line[5] + ' ' + first_line[6]
                        track_end = last_line[5] + ' ' + last_line[6]
                        activity_id = activities_counter

                        #Check if there is already an activity with those dates
                        if(labels_dict[id] and track_start + " - " + track_end in activities_id):
                            #Get its trasportation mode
                            trasportation = activities_id[track_start + " - " + track_end]
                            
                        #If there is no extra data, trasportation mode is set to None
                        else:
                            trasportation = None

                        #Add activity id to user
                        user["activities"].append(activity_id)

                        #Create activity, no trackpoints yet
                        activity = {
                            "_id": activity_id,
                            "user_id": id,
                            "transportation_mode": trasportation,
                            "start_date_time": track_start,
                            "end_date_time": track_end,
                            "trackpoints": []
                        }
                        activities_docs.append(activity)
                        activities_counter += 1
                    #Reopen file to restart values
                    with open(root + "/" + file_name, 'r') as file: 
                        #Skip header
                        for i in range(6):
                            next(file)
                        csvreader = csv.reader(file, delimiter=',')
                        for row in csvreader:
                            #Add this trackpoint to the activity                        
                            activity["trackpoints"].append(trackpoint_counter)
                            #Create trackpoint and add it to the list
                            track_values.append({
                                "_id": trackpoint_counter,
                                "activity_id": activity_id,
                                "lat": row[0],
                                "lon": row[1],
                                "altitude": row[3],
                                "date_days": row[4],
                                "date_time": row[5] + ' ' + row[6]
                            })
                            trackpoint_counter += 1

        #If there is data left to insert, insert it
        if(len(track_values) > 0):
            trackpoints_collection.insert_many(track_values)
            activities_collection.insert_many(activities_docs)
            user_collection.insert_many(user_docs)        
        print("All data inserted")
    
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
            program.create_coll(collection_name="Users")
            program.create_coll(collection_name="Activities")
            program.create_coll(collection_name="Trackpoints")
            program.insert_all_documents()
        if args.fetch_all:
            program.fetch_documents(collection_name="Users")
            program.show_coll()
        
        k = 9

        #program.query_library.queries[k]()

        if args.query:
            program.query_library.queries[args.query]()
        if args.drop_all:
            program.drop_coll(collection_name="Users")
            program.drop_coll(collection_name="Activities")
            program.drop_coll(collection_name="Trackpoints")
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
