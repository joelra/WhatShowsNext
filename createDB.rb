require 'rubygems'
require 'json'
require 'net/http'
require 'mysql'

def sanitize(value, type)
   if type == "string"
      value = value.to_s
      if value != ''
         return value.gsub("'", %q(\\\'))
      else
         return "NA"
      end
   elsif type == "int"
      if value == ''
         return '-1'
      else
         return value.to_s
      end
   end
   return "NA"
end

def getList(con, list, status, tableName)
      
      api_key = "kej36g99ry7adxc2f37g7tqq"
      base_url = "http://api.rottentomatoes.com/api/public/v1.0/lists/" + list + ".json?apikey="
      limit = 50
      url = "#{base_url}#{api_key}&limit=#{limit}"
      resp = Net::HTTP.get_response(URI.parse(url))
      data = resp.body
      
      # we convert the returned JSON data to native Ruby
      # data structure - a hash
      result = JSON.parse(data)

      result["movies"].each do |item|
         begin
            # Check for existing row already
            existingRow = con.query("SELECT * FROM " + tableName + " WHERE rotten_id=" + item["id"])
            
            if defined? existingRow.fetch_row.length
               #puts("Updating")
               updateStatement = "UPDATE Movies SET " + getRottenValues(item, "update", status) + " WHERE rotten_id=" + item["id"]
               #puts updateStatement
               con.query(updateStatement)
            else
               createStatement = "INSERT INTO " + tableName + "(created_at, rotten_id, imdb_id, status, title, year, mpaa_rating, runtime, theater_release, dvd_release, \
                                                digital_release, rotten_tomatoes_critics_rating, rotten_tomatoes_critics_score, \
                                                rotten_tomatoes_audience_score, synopsis, artwork, thumbnail, cast_1, cast_2, cast_3, cast_4) \
                           VALUES(" + getRottenValues(item, "create", status) + ");"
               
               #puts createStatement       
               con.query(createStatement)
               #puts("Creating")
            end
         rescue StandardError => bang
            puts "We've hit an error here man" + bang.to_s
         end
      end
   

   # if the hash has 'Error' as a key, we raise an error
   if result.has_key? 'Error'
      raise "web service error"
   end
   puts "Success for " + list
end

def getRottenValues(item, type, status)
   values = ""

   if type == "create"
      values = "'" + Time.now.strftime("%d/%m/%Y %H:%M") + "', "
      values += sanitize(item["id"], "int") + ","
   end
      values += (type == "update" ? "imdb_id=" : "") + sanitize(item["alternate_ids"] ? item["alternate_ids"]["imdb"] : "", "int").to_s + ", \
      " + (type == "update" ? "status=" : "") + "'" + status + "', \
      " + (type == "update" ? "title=" : "") + "'" +  sanitize(item["title"], "string") + "', \
      " + (type == "update" ? "year=" : "") + sanitize(item["year"], "int") + ", \
      " + (type == "update" ? "mpaa_rating=" : "") + "'" +  sanitize(item["mpaa_rating"], "string") + "', \
      " + (type == "update" ? "runtime=" : "") + sanitize(item["runtime"], "int") + ", \
      " + (type == "update" ? "theater_release=" : "") + "'" +  sanitize(item["release_dates"] ? item["release_dates"]["theater"] : "", "string") + "', \
      " + (type == "update" ? "dvd_release=" : "") + "'" +  sanitize(item["release_dates"] ? item["release_dates"]["dvd"] : "", "string") + "', \
      " + (type == "update" ? "digital_release=" : "") + "'" +  sanitize(item["release_dates"] ? item["release_dates"]["digital"] : "", "string") + "', \
      " + (type == "update" ? "rotten_tomatoes_critics_rating=" : "") + "'" +  sanitize(item["ratings"] ? item["ratings"]["critics_rating"] : "", "string") + "', \
      " + (type == "update" ? "rotten_tomatoes_critics_score=" : "") + "'" +  sanitize(item["ratings"] ? item["ratings"]["critics_score"] : "", "string") + "', \
      " + (type == "update" ? "rotten_tomatoes_audience_score=" : "") + "'" +  sanitize(item["ratings"] ? item["ratings"]["audience_score"] : "", "string") + "', \
      " + (type == "update" ? "synopsis=" : "") + "'" +  sanitize(item["synopsis"], "string") + "', \
      " + (type == "update" ? "artwork=" : "") + "'" +  sanitize(item["posters"] ? item["posters"]["original"] : "", "string") + "', \
      " + (type == "update" ? "thumbnail=" : "") + "'" +  sanitize(item["posters"] ? item["posters"]["thumbnail"] : "", "string") + "', \
      " + (type == "update" ? "cast_1=" : "") + "'" +  sanitize((item["abridged_cast"].length >= 1 ? item["abridged_cast"][0]["name"] : ""), "string") + "', \
      " + (type == "update" ? "cast_2=" : "") + "'" +  sanitize((item["abridged_cast"].length >= 2 ? item["abridged_cast"][1]["name"] : ""), "string") + "', \
      " + (type == "update" ? "cast_3=" : "") + "'" +  sanitize((item["abridged_cast"].length >= 3 ? item["abridged_cast"][2]["name"] : ""), "string") + "', \
      " + (type == "update" ? "cast_4=" : "") + "'" + sanitize((item["abridged_cast"].length >= 4 ? item["abridged_cast"][3]["name"] : ""), "string") + "'"

      return values
end

def getTrailers(con, tableName)
   result_set = con.query("SELECT `title` FROM " + tableName)
   n_rows = result_set.num_rows

   n_rows.times do |index|
      title = result_set.fetch_row
      setTrailerValues(con, title, tableName)
   end
   puts "Added all trailers..."
end

def setTrailerValues(con, title, tableName)
   url = "http://trailersapi.com/trailers.json?movie=" + title[0].gsub("%","").gsub(" ", "%20").gsub("'", "")
   # puts url
   # url = "#{base_url}#{api_key}&limit=#{limit}"
   resp = Net::HTTP.get_response(URI.parse(url))
   data = resp.body
   
   # we convert the returned JSON data to native Ruby
   # data structure - a hash
   result = JSON.parse(data)

   updateStatement = "UPDATE " + tableName + " SET trailer_link='" + sanitize((result[0] ? result[0]["code"].match(/src="(.+?)"/)[1] : ""), "string") + "' WHERE title='" + sanitize(title[0], "string") + "'"
   # puts updateStatement
   con.query(updateStatement)


end

def getIMDB(con, tableName)
   result_set = con.query("SELECT `imdb_id` FROM " + tableName)
   n_rows = result_set.num_rows

   n_rows.times do |index|
      id = result_set.fetch_row
      if index != -1
         setIMDBValues(con, id[0], tableName)
      end
   end
   puts "Added IMDB info..."

end

def setIMDBValues(con, id, tableName)
   begin
      url = "http://mymovieapi.com/?id=tt#{id}&type=json&plot=full&episode=1&lang=en-US&aka=full&release=full&business=1&tech=1"
      # puts url
      # url = "#{base_url}#{api_key}&limit=#{limit}"
      resp = Net::HTTP.get_response(URI.parse(url))
      data = resp.body
      
      # we convert the returned JSON data to native Ruby
      # data structure - a hash
      result = JSON.parse(data)

      updateStatement = "UPDATE " + tableName + " SET " + getIMDBValues(result) + " WHERE imdb_id=" + id
      # puts updateStatement
      con.query(updateStatement)
   rescue StandardError => err
      puts "Error parsing json (some server error...)"
   end
end

def getIMDBValues(item)
   budget = "NA"
   if(item["business"])
      if(item["business"]["budget"])
         budget = sanitize(item["business"]["budget"][0]["money"], "string")
      end
   end
   return "imdb_link='" + sanitize(item["imdb_url"], "string") + "', \
            budget='" + budget + "', \
            genres='" + sanitize((item["genres"] ? item["genres"].join(', ') : ""), "string") + "', \
            writers='" + sanitize((item["writers"] ? item["writers"].join(', ') : ""), "string") + "', \
            limited='" + sanitize((item["release_date"] ? item["release_date"][0]["remarks"] : "False") , "string") + "', \
            actors='" + sanitize((item["actors"] ? item["actors"].join(', ') : ""), "string") + "', \
            directors='" + sanitize((item["directors"] ? item["directors"].join(', ') : ""), "string") + "'"

end

def create_table(con, tableName)
   con.query("DROP TABLE IF EXISTS " + tableName)
   con.query("CREATE TABLE IF NOT EXISTS \
      " + tableName + "( Id INT PRIMARY KEY AUTO_INCREMENT, \
               updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
               created_at VARCHAR(255), \
               rotten_id INT(11), \
               imdb_id INT(11), \
               status VARCHAR(255), \
               title VARCHAR(255), \
               year INT(11), \
               mpaa_rating VARCHAR(255), \
               runtime INT(11), \
               theater_release VARCHAR(255), \
               dvd_release VARCHAR(255), \
               digital_release VARCHAR(255), \
               rotten_tomatoes_critics_rating VARCHAR(255), \
               rotten_tomatoes_critics_score VARCHAR(255), \
               rotten_tomatoes_audience_score VARCHAR(255), \
               synopsis TEXT, \
               artwork VARCHAR(255), \
               thumbnail VARCHAR(255), \
               directors VARCHAR(255), \
               writers VARCHAR(255), \
               budget VARCHAR(255), \
               genres VARCHAR(255), \
               limited VARCHAR(255), \
               actors TEXT, \
               trailer_link VARCHAR(255), \
               trailer_checked VARCHAR(255), \
               imdb_link VARCHAR(255), \
               cast_1 VARCHAR(255), \
               cast_2 VARCHAR(255), \
               cast_3 VARCHAR(255), \
               cast_4 VARCHAR(255))")
end

con = Mysql.new 'localhost', 'root', 'welcome08', 'RottenTomatoes'

begin
   create_table(con, "Movies")
   create_table(con, "DVDs")
   create_table(con, "Archives")

   getList(con, "movies/opening", "opening", "Movies")
   getList(con, "movies/in_theaters", "in_theaters", "Movies")
   getList(con, "movies/upcoming", "upcoming", "Movies")
   getList(con, "dvds/current_releases", "current_release", "Movies")
   getList(con, "dvds/new_releases", "new_release", "Movies")
   getList(con, "dvds/upcoming", "upcoming_release", "Movies")

   getIMDB(con, "Movies")
   getTrailers(con, "Movies")
rescue Error
   con.close()
end

con.close()


