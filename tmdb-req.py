import requests
import mysql.connector


db = mysql.connector.connect(
  host="localhost",
  user="root",
  database="scrape",
  password="PASSWORD"
)

cursor = db.cursor(buffered=True)

cursor.execute("SELECT DISTINCT imdb_id FROM torrents")

imdbIDs = [x[0] for x in cursor.fetchall()]

tmdbMovies = []
for imdbID in imdbIDs:
    tmdbInfo = {}
    movies = requests.get("https://api.themoviedb.org/3/find/{}?api_key=b192b9bc652a5236a79376b60d36169c&language=en-US&external_source=imdb_id".format(imdbID)).json()['movie_results']

    for movie in movies: #Should just be 1 movie, but this loop accounts for multiple movies with same imdb_id

        tmdbInfo["id"] = movie["id"]
        tmdbInfo['imdbID'] = imdbID
        tmdbInfo["title"] = movie["title"]
        tmdbInfo["keywords"] = []

        keywords = requests.get("https://api.themoviedb.org/3/movie/{}/keywords?api_key=b192b9bc652a5236a79376b60d36169c".format(movie["id"])).json()['keywords']

        for keyword in keywords:
            tmdbInfo["keywords"].append(keyword["name"])

    tmdbMovies.append(tmdbInfo)

for record in tmdbMovies:
    if(bool(record)):
        sql = "INSERT into tmdb_movies (id, imdb_id, movie_title, keywords) VALUES (%s, %s, %s, %s)"
        values = (record["id"], record["imdbID"], record["title"], ",".join(record["keywords"]))
        cursor.execute(sql, values)


db.commit()
