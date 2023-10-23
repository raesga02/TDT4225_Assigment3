import tabulate
import datetime
from haversine import haversine, Unit
from pprint import pprint 

'''
This query library works with the following schema:

'''

class QueriesLibrary():

    def __init__(self, _db):
        self.db = _db
        self.users = self.db["Users"]
        self.activities = self.db["Activities"]
        self.trackpoints = self.db["Trackpoints"]

        self.queries = {
            1: self.query_1,
            2: self.query_2,
            3: self.query_3,
            4: self.query_4,
            5: self.query_5,
            6: self.query_6,
            7: self.query_7,
            8: self.query_8,
            9: self.query_9,
            10: self.query_10,
            11: self.query_11,
        }


    def query_1(self):
        '''
        How many users, activities and trackpoints are there in the dataset 
        (after it is inserted into the database).
        '''
        print("Query 1: ")
        #Get number of documents for each table
        num_users = self.users.count_documents({})
        num_activities = self.activities.count_documents({})
        num_trackpoints = self.trackpoints.count_documents({})

        #Print results
        print(f"Number of users: {num_users}")
        print(f"Number of activities: {num_activities}")
        print(f"Number of trackpoints: {num_trackpoints}")

    def query_2(self):
        '''
        Find the average number of activities per user.
        '''
        print("Query 2: ")
        avarage_activities = self.users.aggregate([
            #Get the number of trackpoints ids in the array
            {
                '$project': 
                {
                    'activities_size': {'$size': '$activities'}
                }
            },
            #Group all the activities making the average of the number of trackpoints ids in the array
            {
                "$group": {
                    "_id": None,
                    "avg_activities": { "$avg": "$activities_size"}
                }
            },
            #Dont show id that is just None
            {
                "$unset": "_id" 
            }
            
        ])
        #Print results
        for result in avarage_activities: pprint(result)

    def query_3(self):
        '''
        Find the top 20 users with the highest number of activities.
        '''
        print("Query 3: ")
        top_users = self.users.aggregate([
            # Get the number of activities of each user
            {
                '$project': {
                    'activity_amount': { '$size': '$activities'},
                    'user_id': '$_id'
                }

            },
            # Order the list by the number of activities
            {
                '$sort': {
                    'activity_amount': -1
                }
            },
            # Limit the output to only 20 documents
            {
                '$limit': 20
            },
            {
                '$unset': '_id'
            }
        ])

        for result in top_users: pprint(result)


    def query_4(self):
        '''
        List all users that have taken a taxi
        '''
        print("Query 4: ")
        users_taxi = self.activities.aggregate([
            {
                "$match": { "transportation_mode": "taxi" }
            },
            {
                "$group": { "_id": "$user_id" }
            },
            {
                "$project": 
                { 
                    "user_id": "$_id",
                    "_id": 0
                }
            }
        ])
        #Print results
        print("Users that have taken a taxi: ")
        for result in users_taxi: pprint(result)



    def query_5(self):
        '''
        Find all types of transportation modes and count the number of activities
        for each transportation mode (do not count activities with mode = null)
        '''
        print("Query 5: ")
        number_activities_trackpoints = self.activities.aggregate([
            #Group activities by the transportation mode
            {
                "$group": 
                {
                    "_id": "$transportation_mode",
                    "number_activities": { "$sum": 1}
                }
            },
            #Delete if transportation mode is none
            {
                
                "$match": { "_id": { "$ne": None } }
            },
            #Rename the columns
            {
                "$project": { "number_activities": "$number_activities", "transportation_mode": "$_id" }
            },
            #Delete _id because now it is shown as transportation_mode
            {
                "$unset": "_id" 
            }
        ])
        #Print results
        for result in number_activities_trackpoints: pprint(result)


    def query_6(self):
        '''
        a) Find the year with the most activities
        b) Is this also the year with most recorded hours?
        '''
        print("Query 6: ")

        # a) Find the year with most activities
        year_most_activities = self.activities.aggregate([
            # Extract the year of each activity
            {
                '$project': {
                    'year': {'$year': '$start_date_time'}
                }
            },
            # Count the number of activities for each year
            {
                '$group': {
                    '_id': '$year',
                    'num_activities': { '$sum': 1 }
                }
            },
            # Sort the years by descending number of activities
            {
                '$sort': {
                    'num_activities': -1
                }
            },
            # Show only 1 document (the year with the most activities)
            {
                '$limit': 1
            },
            {
                '$project': {
                    'year': '$_id',
                    'num_activities': '$num_activities'
                }
            },
            {
                '$unset': '_id'
            }
        ])

        # Find the year with most recorded hours
        year_most_recorded_hours = self.trackpoints.aggregate([
            {
                '$project': {
                    'date': '$date_time'
                }
            },
            # Extract the year, day of the year and hour for each trackpoint
            {
                '$project': {
                    'year': {
                        '$year': '$date'
                    },
                    'day_of_year' : {
                        '$dayOfYear': '$date'
                    },
                    'hour' : {
                        '$hour': '$date'
                    }
                },
            },
            # Group the trackpoints by year-dayofyear-hour (disregard the hours)
            {
                '$group': {
                    '_id': { 'year': '$year', 'day_of_year': '$day_of_year', 'hour': '$hour'}
                }
            },
            # Count the amount of recorded hours for each group
            {
                '$group': {
                    '_id': '$_id.year',
                    'num_recorded_hours': { '$sum': 1 },
                }
            },
            # Sort the result by descending value of the sum
            {
                '$sort': {
                    'num_recorded_hours': -1
                }
            },
            # Show only one
            {
                '$limit': 1
            },
            {
                '$project': {
                    'year': '$_id',
                    'num_recorded_hours': '$num_recorded_hours'
                }
            },
            {
                '$unset': '_id'
            }
        ])

        for result in year_most_activities: pprint(result)
        for result in year_most_recorded_hours: pprint(result)


    def query_7(self):
        '''
        Find the total distance (in km) walked in 2008 by user with id = 112
        '''
        print("Query 7: ")
        # print all user 112 activities:
        activities_112 = self.activities.aggregate([
            {
                "$match": { "user_id": 112 , "transportation_mode": "walk" }
            },
            {
                "$project":
                {
                    "activity_id": "$_id",
                    "_id": 0,
                    "trackpoints": "$trackpoints",
                }
            },
            {
                "$lookup":
                {
                    "from": "Trackpoints",
                    "localField": "trackpoints",
                    "foreignField": "_id",
                    "as": "trackpoints",
                    "pipeline": [
                        {
                            "$match": { "date_time": { "$regex": "2008" } }
                        },
                        {
                            "$project":
                            {
                                "_id": 0,
                                "date_time": "$date_time",
                                "lat": "$lat",
                                "lon": "$lon",
                            }
                        },
                        {
                            "$sort": { "date_time": 1 }
                        }
                    ]
                }
            },
            {
                "$sort": { "activity_id": 1 }
            }
            ])
        
        #for activity in activities_112: pprint(activity)

        # For each activity, calculate the distance between each trackpoint on the array of trackpoints
        # and sum it to the total distance
        total_distance = 0
        for activity in activities_112:
            for i in range(len(activity["trackpoints"])-1):
                distance = haversine((activity["trackpoints"][i]["lat"], activity["trackpoints"][i]["lon"]), (activity["trackpoints"][i+1]["lat"], activity["trackpoints"][i+1]["lon"]), unit=Unit.KILOMETERS)
                total_distance += distance
        #Print results
        print(f"Total distance walked by user 112 in 2008: {total_distance} km")
           


    def query_8(self):
        '''
        Find the top 20 users who have gained the most altitude meter
        '''
        print("Query 8: ")
        activities_with_altitudes = self.activities.aggregate([
            #Join activities with its trackpoints
            {
                '$lookup':
                {
                    'from':'Trackpoints',
                    'localField':'trackpoints',
                    'foreignField':'_id',
                    'as':'activity_trackpoints'
                }
            },
            #Separate the data to be able to use it
            { "$unwind": "$activity_trackpoints" },
            #Rejoin the data but now in the correct order
            { "$group":
                {
                    "_id": {"_id":"$_id","user_id": "$user_id"},
                    "altitudes": {"$push": "$activity_trackpoints.altitude"}
                } 
            },
            #Cleanly display data to be able to use it in python easily
            {
                "$project":
                {
                    "_id": "$_id._id",
                    "user_id": "$_id.user_id",
                    "altitudes": "$altitudes"
                }
            }
            ]);
        
        user_max_altitude = dict()
        #Iterate over the activities and calculate altitude difference
        for activity in activities_with_altitudes: 
            #Init dictionaries value
            if(activity["user_id"] not in user_max_altitude):
                user_max_altitude[activity["user_id"]] = 0
            
            #Calculate dif
            altitude_dif = 0
            previous = None
            for altitude in activity["altitudes"]:
                if int(altitude) == -7777:
                    continue
                if(previous is not None and int(altitude) > previous):
                    altitude_dif += int(altitude) - previous
                previous = int(altitude)
            
            if(user_max_altitude[activity["user_id"]] < altitude_dif):
                user_max_altitude[activity["user_id"]] = altitude_dif
        
        #Get top 20 users
        user_max_altitude_filtered = dict(sorted(user_max_altitude.items(), key = lambda x: x[1], reverse = True)[:20])
        #Put the results cleanly
        results = list()
        for key in user_max_altitude_filtered:
            results.append({
                "id": key,
                "total_meters_gained": user_max_altitude_filtered[key]
            })
        #Print results
        for result in results: pprint(result)




    def query_9(self):
        """
        Find all users who have invalid activities, and their number of invalid
        activities
        -> An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate with at least 5 minutes.
        """
        print("Query 9: ")
        activities_with_date = self.activities.aggregate([
            #Join activities with its trackpoints
            {
                '$lookup':
                {
                    'from':'Trackpoints',
                    'localField':'trackpoints',
                    'foreignField':'_id',
                    'as':'activity_trackpoints'
                }
            },
            #Separate the data to be able to use it
            { "$unwind": "$activity_trackpoints" },
            #Rejoin the data but now in the correct order
            { "$group":
                {
                    "_id": {"_id":"$_id","user_id": "$user_id"},
                    "date_time": {"$push": "$activity_trackpoints.date_time"}
                } 
            },
            #Cleanly display data to be able to use it in python easily
            {
                "$project":
                {
                    "_id": "$_id._id",
                    "user_id": "$_id.user_id",
                    "date_time": "$date_time"
                }
            }
            ]);
        
        user_invalids = dict()
        five_minutes = 5*60

        #Iterate over the activities and calculate check the trackpoints time
        for activity in activities_with_date: 
            #Calculate difference between one trackpoint and the previous 
            previous_time = None
            invalid = False
            for trackpoint_time in activity["date_time"]: 
                #If invalid, mark it and stop looping
                if(previous_time is not None and (trackpoint_time - previous_time).total_seconds() >= five_minutes):
                    invalid = True
                    break
                previous_time = trackpoint_time
            #If invalid add the count to 
            if(invalid):
                if(activity["user_id"] in user_invalids):
                    user_invalids[activity["user_id"]] += 1
                else:
                    user_invalids[activity["user_id"]] = 1
        
        #Put the results cleanly and order it by user id
        user_invalids_orderes = dict(sorted(user_invalids.items()))
        results = list()
        for key in user_invalids_orderes:
            results.append({
                "user_id": key,
                "number_invalid_activities": user_invalids[key]
            })
        #Print results
        for result in results: pprint(result)


    def query_10(self):
        """
        Find the users who have tracked an activity in the Forbidden City
        (lat = 39.916, lon = 116.397)
        """
        print("Query 10: ")
        users_forbidden_city = self.activities.aggregate([
            {
                "$project":
                {
                    "activity_id": "$_id",
                    "user_id": "$user_id",
                    "_id": 0,
                    "trackpoints": "$trackpoints",
                }
            },
            {
                "$lookup":
                {
                    "from": "Trackpoints",
                    "localField": "trackpoints",
                    "foreignField": "_id",
                    "as": "trackpoints",
                    "pipeline": [
                        {
                            "$match": 
                            { 
                                "lat": {"$gt": 39.915, "$lt": 39.917},
                                "lon": {"$gt": 116.396, "$lt": 116.398}
                            }
                        }
                    ]
                }
            },
            {
                "$unwind": "$trackpoints"
            },
            {
                "$group": 
                { 
                    "_id": "$user_id"
                }
            },
            {
                "$project":
                {
                    "user_id": "$_id",
                    "_id": 0,
                }
            }
            ])
        
        #Print results
        print("Users that have been in the Forbidden City: ")
        for result in users_forbidden_city: pprint(result)

        
    def query_11(self):
        '''
        Find all users who have registered transportation_mode and their most used
        transportation_mode
        '''
        print("Query 11: ")
        users_trasportation = self.users.aggregate([
            #Delete if transportation mode is false
            {
                "$match": { "has_label": { "$ne": False }}
            },
            #Join its activities
            {
                '$lookup':
                {
                    'from':'Activities',
                    'localField':'activities',
                    'foreignField':'_id',
                    'as':'activity_values'
                }
            },
            #Keep just the data we want
            { 
                "$group": 
                { 
                    "_id" : {"_id": "$_id", "transportation_modes": "$activity_values.transportation_mode"} 
                }     
            },
            #Reorder data for easier use and filter Nones out of the array
            {
                "$project":
                {
                    "_id": "$_id._id",
                    "transportation_modes": 
                    {
                        "$filter": 
                        {
                            "input": "$_id.transportation_modes",
                            "as": "item",
                            "cond": {"$ne": ["$$item", None]}
                        }}
                }
            },
            #Eliminate users with empty arrays
            {   "$match": 
                {
                    "transportation_modes": {"$exists": True, "$ne": []}
                }
            },
            #Separate the array into different documents
            { "$unwind": "$transportation_modes" },
            #Regroup by id and trasportation mode doing the count
            { 
                "$group": 
                { 
                    "_id" : {"_id": "$_id", "transportation_mode": "$transportation_modes"} ,
                    "number_times_used": {"$sum": 1}
                }     
            },
            #Reorder data for easier use
            {
                "$project": 
                {
                    "_id": "$_id._id",
                    "transportation_mode": "$_id.transportation_mode",
                    "number_times_used": "$number_times_used"
                }
            },
            #Sort data to be able to do the next step
            { "$sort": { "_id": 1, "number_times_used": -1 } },
            #Filter, just keep the document with the most used trasportation mode (using first since is sorted)
            { 
                "$group": 
                { 
                    "_id": "$_id", 
                    "most_used_transportation_mode": { "$first": "$$ROOT.transportation_mode" } 
                } 
            },
            #Sort data for correct display
            { "$sort": { "_id": 1} }
            
        ])
        #Print results
        for result in users_trasportation: pprint(result)