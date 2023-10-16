import tabulate
import datetime
from haversine import haversine, Unit

'''
This query library works with the following schema:

'''

class QueriesLibrary():

    def __init__(self, _collection):
        self.collection = _collection
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
        


    def query_2(self):
        '''
        Find the average number of activities per user.
        '''
        print("Query 2: ")


    def query_3(self):
        '''
        Find the top 20 users with the highest number of activities.
        '''
        print("Query 3: ")


    def query_4(self):
        '''
        List all users that have taken a taxi
        '''
        print("Query 4: ")


    def query_5(self):
        '''
        Find all types of transportation modes and count the number of activities
        for each transportation mode (do not count activities with mode = null)
        '''
        print("Query 5: ")
    

    def query_6(self):
        '''
        a) Find the year with the most activities
        b) Is this also the year with most recorder hours?
        '''
        print("Query 6: ")


    def query_7(self):
        '''
        Find the total distance (in km) walked in 2008 by user with id = 112
        '''
        print("Query 7: ")
    


    def query_8(self):
        '''
        Find the top 20 users who have gained the most altitude meter
        '''
        print("Query 8: ")


    def query_9(self):
        """
        Find all users who have invalid activities, and their number of invalid
        activities
        -> An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate with at least 5 minutes.
        """
        print("Query 9: ")


    def query_10(self):
        """
        Find the users who have tracked an activity in the Forbidden City
        (lat = 39.916, lon = 116.397)
        """
        print("Query 10: ")

        
    def query_11(self):
        '''
        Find all users who have registered transportation_mode and their most used
        transportation_mode
        '''
        print("Query 11: ")