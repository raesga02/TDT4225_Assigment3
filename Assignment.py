import csv
import os
from pprint import pprint 
from DbConnector import DbConnector


class Geolife:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    """
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
    
    
    
    user = 
    {
        "_id": --,
        "has_label": --,
        "activities": 
        [
            {
                "_id": --,
                "transportation_mode": --,
                "start_date_time": --,
                "end_date_time"--,
                "trackpoints":
                [
                    {
                        "_id": --,
                        "lat": --,
                        "lon": --,
                        "altitude": --,
                        "date_days": --,
                        "date_time": --
                    }
                ]
            }
        
        ]
    }
    ]
    """

    def insert_documents(self, collection_name):

        collection = self.db[collection_name]
        users = []

        #Get the users that have labels
        labels_list = open("./dataset/labeled_ids.txt", "r").read().split("\n")
        labels_list.remove("")
        labels_list = [int(label) for label in labels_list]
        labels_dict = dict.fromkeys(labels_list,True)

        #Variables to keep track of activities ids 
        activities_id = dict()
        activities_counter = 1

        #Control variables
        skip_first = True

        id = -1
        counter = 0

        #Iterate the database
        for (root,dirs,files) in os.walk('./dataset/Data', topdown=True):
            
            #Skip the first iteration that shows all the data directories
            if(skip_first):
                skip_first = False 
                continue
            
            #If there is a lot of users to add, add them so we dont kill the database
            if(len(users) > 10):
                collection.insert_many(users)
                users = []
            
            #If we are in the users folder
            if("Trajectory" in dirs):

                #Get the user id
                id = int(root.split("dataset/Data/")[1])
                
                #If the user is not in the dictionary add it as false
                if(id not in labels_dict):
                    labels_dict[id] = False
                
                #Create the user 
                user = {
                    "_id": id,
                    "has_label": labels_dict[id],
                    "activities": []
                }

                users.append(user)

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
                            activity = {
                                "_id": activities_counter,
                                "transportation_mode": trasportation,
                                "start_date_time": track_start,
                                "end_date_time": track_end,
                                "trackpoints": []
                            }
                            activities_counter += 1
                            #
                        #If there is no extra data, create it without trasportation mode
                        else:
                            activity = {
                                "_id": activities_counter,
                                "transportation_mode": None,
                                "start_date_time": track_start,
                                "end_date_time": track_end,
                                "trackpoints": []
                            }
                            activities_counter += 1

                    #Reopen file to restart values
                    with open(root + "/" + file_name, 'r') as file: 
                        track_values = list()
                        for i in range(6):
                            next(file)
                        csvreader = csv.reader(file, delimiter=',')
                        for row in csvreader:
                            #Add this trackpoint to the activity                        
                            activity["trackpoints"].append({
                                "lat": row[0],
                                "lon": row[1],
                                "altitude": row[3],
                                "date_days": row[4],
                                "date_time": row[5] + ' ' + row[6]
                            })

                    #Add the activity to the user
                    user["activities"] = activity

        #Add the users that were not added before
        collection.insert_many(users)         
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
         


def main():
    program = None
    try:
        program = Geolife()
        program.create_coll(collection_name="Users")
        program.show_coll()
        program.insert_documents(collection_name="Users")
        program.fetch_documents(collection_name="Users")
        program.drop_coll(collection_name="Users")
        # program.drop_coll(collection_name='person')
        # program.drop_coll(collection_name='users')
        # Check that the table is dropped
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
