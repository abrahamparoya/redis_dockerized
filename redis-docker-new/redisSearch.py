import redis
import csv
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
    print("QUERY 1:")
    tomID = find_actor_by_name(name).docs[0]["id"]
    moviesList = find_actors_roles(tomID).docs
    for movies in moviesList:
        newMovie = find_movie_by_ID(movies["movieID"]).docs
        print(newMovie[0]["title"])
def q2(name):
    
    """
    Find all characters played by Tom Hardy
    """
    print("")
    print("QUERY 2:")
    tomID = find_actor_by_name(name).docs[0]["id"]
    moviesList = find_actors_roles(tomID).docs
    for movies in moviesList:
        print(movies["role"])
def q3(name):
    tomID = find_actor_by_name(name).docs[0]["id"]
    print("")
    print("QUERY 3:")
    moviesList = find_actors_roles(tomID).docs
    actorIDList = []
    for movies in moviesList:
        actors = find_actors_by_movie(movies["movieID"]).docs
        for actor in actors:
            actorIDList.append(actor["actorID"])
    for ID in actorIDList:
        actor = find_actor_by_ID(ID).docs
    print("")
    print("People who have worked with", name + ":")
    for ID in actorIDList:
        if(ID == tomID):
            continue
        actor = find_actor_by_ID(ID).docs
        print(actor[0]["name"])
def q4(genre):
    movieList = find_movies_by_genre(genre).docs
    print(len(movieList))
    directorIDList = []
    for movies in movieList:
        contributors = find_roles_by_movie(movies["movieID"]).docs
        for roles in contributors:
            if roles["category"] == "director" and roles["actorID"] not in directorIDList:
                directorIDList.append(roles["actorID"])
    print(len(directorIDList))
def q5(year1, year2):
    print("")
    print("QUERY 5:")
    actorList1 = find_actor_by_birthyear(year1).docs
    actorList2 = find_actor_by_birthyear(year2).docs
    print("All people born between", year1, "and",year2 +":")
    for actors in actorList1:
        print(actors["name"])
    for actors in actorList2:
        print(actors["name"])
def q6(name, year):
    """
    Find all people born in 1975 who have worked with Will Ferrell
    """
    print("")
    print("QUERY 6:")
    willID = find_actor_by_name(name).docs
    for will in willID:
        if will["birthYear"] == "1967":
            willID = will["id"]
            break
    moviesList = find_actors_roles(willID).docs
    actorIDList = []
    for movies in moviesList:
        actor = find_actors_by_movie(movies["movieID"]).docs
        for actors in actor:
            newID = actors["actorID"]
            if(newID == willID):
                continue
            actorData = find_actor_by_ID(newID).docs
            if newID not in actorIDList and actorData[0]["birthYear"]== year:
                actorIDList.append(newID)
    for ID in actorIDList:
        print(find_actor_by_ID(ID).docs[0]["name"])
def q7(name1, name2):
    """
    Find James G. and Tom Hardy collaborations
    """
    tomID = find_actor_by_name(name1).docs[0]["id"]
    tomRoles = find_actors_roles(tomID).docs
    jamesID = find_actor_by_name(name2).docs[0]["id"]
    jamesRoles = find_actors_roles(jamesID).docs
    collabList = []
    for tom in tomRoles:
        for james in jamesRoles:
            if james["movieID"] == tom["movieID"]:
                title = find_movie_by_ID(james["movieID"]).docs[0]["title"]
                collabList.append(title)
    print("")
    print("QUERY 7:", name1 +"'s and", name2 +"'s collaborations:")
    print(collabList)
def main():
    global movieDB
    movieDB = redis.Redis(host='redis-12721.c323.us-east-1-2.ec2.cloud.redislabs.com', port=12721, decode_responses=True , password= '9uvYJ66YYXwrqeu8SPyJ9uB1y8Mv2KZd')
    q1("Tom Hardy")
    #q2("Tom Hardy")
    #q3("Tom Hardy")
    # q4("Fantasy")
    #q5("1975","1976")
    #q6("Will Ferrell", "1975")
    #q7("Tom Hardy", "James Gandolfini")
if __name__ == "__main__":
    main()