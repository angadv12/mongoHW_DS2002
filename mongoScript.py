# Name: Angad Brar, Computing ID: zqq4hx

from pymongo import MongoClient
import os
from dotenv import load_dotenv

username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
# Connect to MongoDB
client = MongoClient(f"mongodb+srv://{username}:{password}@sample_mflix.oadjx53.mongodb.net/test?retryWrites=true&w=majority")
db = client.sample_mflix
collection = db.movies


# Exercise 1: Basic Searching and Filtering

action_movie = collection.find_one({"genres": "Action"})
print("First Action Movie:", action_movie)

movies_after_2000 = collection.find({"year": {"$gt": 2000}}).limit(5)
print("Movies after 2000:")
for movie in movies_after_2000:
    print(movie)

high_rated_movies = collection.find({"imdb.rating": {"$gt": 8.5}}).limit(5)
print("High Rated Movies:")
for movie in high_rated_movies:
    print(movie)

action_adventure_movies = collection.find({"genres": {"$all": ["Action", "Adventure"]}}).limit(5)
print("Action and Adventure Movies:")
for movie in action_adventure_movies:
    print(movie)


# Exercise 2: Sorting Results

sorted_comedy_movies = collection.find({"genres": "Comedy"}).sort("imdb.rating", -1).limit(5)
print("Top Comedy Movies by IMDb Rating:")
for movie in sorted_comedy_movies:
    print(movie)

sorted_drama_movies = collection.find({"genres": "Drama"}).sort("year", 1).limit(5)
print("Oldest Drama Movies:")
for movie in sorted_drama_movies:
    print(movie)


# Exercise 3: Aggregation Pipeline

avg_rating_by_genre = collection.aggregate([
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres", "avg_rating": {"$avg": "$imdb.rating"}}},
    {"$sort": {"avg_rating": -1}},
    {"$limit": 5}
])
print("Top Genres by Average IMDb Rating:")
for genre in avg_rating_by_genre:
    print(genre)

top_directors = collection.aggregate([
    {"$unwind": "$directors"},
    {"$group": {"_id": "$directors", "avg_rating": {"$avg": "$imdb.rating"}}},
    {"$sort": {"avg_rating": -1}},
    {"$limit": 5}
])
print("Top Directors by Average IMDb Rating:")
for director in top_directors:
    print(director)

movies_per_year = collection.aggregate([
    {"$group": {"_id": "$year", "total_movies": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
])
print("Movies Released Per Year:")
for year in movies_per_year:
    print(year)


# Exercise 4: Updating and Deleting Documents

collection.update_one({"title": "The Godfather"}, {"$set": {"imdb.rating": 9.5}})
print("Updated 'The Godfather' rating to 9.5")

collection.update_many({"genres": "Horror", "imdb.rating": {"$exists": False}}, {"$set": {"imdb.rating": 6.0}})
print("Updated IMDb rating of Horror movies to 6.0 if null")

deleted_count = collection.delete_many({"year": {"$lt": 1950}}).deleted_count
print(f"Deleted {deleted_count} movies released before 1950")


# Exercise 5: Text Search

collection.create_index([("title", "text")])

love_movies = collection.find({"$text": {"$search": "love"}})
print("Movies with 'love' in the title:")
for movie in love_movies:
    print(movie)

collection.create_index([("title", "text"), ("plot", "text")])  # create combined text index
war_movies = collection.find({"$text": {"$search": "war"}}).sort("imdb.rating", -1).limit(5)
print("Top 'War' Movies by IMDb Rating:")
for movie in war_movies:
    print(movie)


# Exercise 6: Combining Multiple Queries (Bonus)

action_high_rated_movies = collection.find(
    {"genres": "Action", "imdb.rating": {"$gt": 8}}
).sort("year", -1)
print("Top Rated Action Movies (IMDb > 8):")
for movie in action_high_rated_movies:
    print(movie)

nolan_movies = collection.find(
    {"directors": "Christopher Nolan", "imdb.rating": {"$gt": 8}}
).sort("imdb.rating", -1).limit(3)
print("Top Christopher Nolan Movies by IMDb Rating:")
for movie in nolan_movies:
    print(movie)
