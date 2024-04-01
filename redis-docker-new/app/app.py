from flask import Flask, redirect, url_for , render_template, request
import redis
import csv
import sys
from redis.commands.search.field import TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType

def find_actor_by_name(name):
    q = f"@name:{name}"
    results = movieDB.ft("idx:actors").search(q)
    return results
def find_movies_by_genre(genre):
    q = f"@genre:{genre}"
    results = movieDB.ft("idx:movies").aggregate(q)
    return results
def find_actors_by_movie(ID):
    q = f"@movieID:{ID}"
    results = movieDB.ft("idx:roles").search(q)
    return results
def find_actor_by_ID(ID):
    q = f"@id:{ID}"
    results = movieDB.ft("idx:actors").search(q)
    return results
def find_actors_roles(ID):
    q = f"@actorID:{ID}"
    results = movieDB.ft("idx:roles").search(q)
    return results
def find_actor_by_birthyear(birthYear):
    q = f"@birthYear:{birthYear}"
    results = movieDB.ft("idx:actors").search(q)
    return results
def find_roles_by_type(role):
    q = f"@category:{role}"
    results = movieDB.ft("idx:roles").search(q)
    return results
def find_movie_by_ID(ID):
    q = f"@movieID:{ID}"
    results = movieDB.ft("idx:movies").search(q)
    return results
def find_roles_by_movie(ID):
    q = f"@movieID:{ID}"
    results = movieDB.ft("idx:roles").search(q)
    return results

def q1(name):
    movs =[]
    # print(name)
    tomID = find_actor_by_name(name).docs[0]["id"]
    moviesList = find_actors_roles(tomID).docs
    for movies in moviesList:
        newMovie = find_movie_by_ID(movies["movieID"]).docs
        movs.append(newMovie[0]["title"])
        # print(newMovie[0]["title"])

    return movs

app = Flask(__name__)

global movieDB
movieDB = redis.Redis(host='redis-12721.c323.us-east-1-2.ec2.cloud.redislabs.com', port=12721, decode_responses=True , password= '9uvYJ66YYXwrqeu8SPyJ9uB1y8Mv2KZd')

@app.route("/", methods=["POST", "GET"])
def home():
    if request.method == "POST":
        user = request.form["nm"]
        return redirect(url_for("user", usr=user))
    else:
        return render_template("home.html")

@app.route("/<usr>")

def user(usr):

    mlist = []
    mlist = q1(usr)  

    return render_template("results.html", content=mlist, actor=usr)
    

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8000)
