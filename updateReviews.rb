require 'rubygems'
require 'json'
require 'net/http'
require 'mysql'

def updateReviews(rotten_id)
	api_key = "kej36g99ry7adxc2f37g7tqq"
	url = "http://api.rottentomatoes.com/api/public/v1.0/movies/#{rotten_id}/reviews.json?apikey=#{api_key}"



end


con = Mysql.new 'movieinstance.cnybahrpes0y.us-west-2.rds.amazonaws.com', 'root', 'welcome08', 'myMovies'



getList(con, "movies/opening", "opening", "Movies")
getList(con, "movies/in_theaters", "in_theaters", "Movies")
getList(con, "movies/upcoming", "upcoming", "Movies")
getList(con, "dvds/current_releases", "current_release", "Movies")
getList(con, "dvds/new_releases", "new_release", "Movies")
getList(con, "dvds/upcoming", "upcoming_release", "Movies")

getIMDB(con, "Movies")
getTrailers(con, "Movies")

con.close()