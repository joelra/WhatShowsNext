require 'rubygems'
require 'json'
require 'net/http'
require 'mysql'

def main ()
   $con = Mysql.new 'localhost', 'regist6_movies', 'o7_r7S{(_-Rp', 'myMovies'
   $updateMessage = "Successfully Updated Database \n\n Output--------------\n"
   $api_key = "kej36g99ry7adxc2f37g7tqq"
   getList("movies/opening", "opening", "Movies")
   getList("movies/in_theaters", "in_theaters", "Movies")
   getList("movies/upcoming", "upcoming", "Movies")
   getList("dvds/current_releases", "current_release", "Movies")
   getList("dvds/new_releases", "new_release", "Movies")
   getList("dvds/upcoming", "upcoming_release", "Movies")

   getIMDB("Movies")
   getTrailers("Movies")
   getReviews("Reviews")

   setUpdate()

   $con.close()
end

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

def getList(list, status, tableName)
   base_url = "http://api.rottentomatoes.com/api/public/v1.0/lists/" + list + ".json?apikey="
   limit = 50
   url = "#{base_url}#{$api_key}&limit=#{limit}"
   resp = Net::HTTP.get_response(URI.parse(url))
   data = resp.body
   result = JSON.parse(data)

   result["movies"].each do |item|
      begin
         existingRow = $con.query("SELECT * FROM " + tableName + " WHERE rotten_id=" + item["id"])
         
         if defined? existingRow.fetch_row.length
            updateStatement = "UPDATE Movies SET " + getRottenValues(item, "update", status) + " WHERE rotten_id=" + item["id"]
            puts "Updating rotten values for " + item["title"]
            $con.query(updateStatement)
         else
            createStatement = "INSERT INTO " + tableName + "(created_at, rotten_id, imdb_id, status, title, year, mpaa_rating, runtime, theater_release, dvd_release, \
                                             digital_release, rotten_tomatoes_critics_rating, rotten_tomatoes_critics_score, \
                                             rotten_tomatoes_audience_score, synopsis, artwork, thumbnail, cast_1, cast_2, cast_3, cast_4) \
                        VALUES(" + getRottenValues(item, "create", status) + ");"
            
            
            puts "Creating rotten values for " + item["title"]
            $updateMessage += "Added " + item["title"] + "\n"
            $con.query(createStatement)
         end
      rescue StandardError => bang
         $updateMessage += "We've hit an error here man" + bang.to_s + "\n"
      end
   end
   puts "Success for " + list
end

def getRottenValues(item, type, status)
   values = ""
   if type == "create"
      values = "'" + Time.now.strftime("%d/%m/%Y %H:%M") + "', "
      values += sanitize(item["id"], "int") + ","
   end
      return values + (type == "update" ? "imdb_id=" : "") + sanitize(item["alternate_ids"] ? item["alternate_ids"]["imdb"] : "", "int").to_s + ", \
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
end

def getTrailers(tableName)
   result_set = $con.query("SELECT `title`, `trailer_checked` FROM " + tableName)
   n_rows = result_set.num_rows

   n_rows.times do |index|
      row = result_set.fetch_row
      if row[1] == "true"
         puts row[0] + "'s trailer has been verified accurate, skipping"
      else
         puts "Updating trailer for " + row[0]
         setTrailerValues(row[0], tableName)
      end
   end
   puts "Added all trailers..."
end

def setTrailerValues(title, tableName)
   begin
      url = "http://trailersapi.com/trailers.json?movie=" + title[0].gsub("%","").gsub(" ", "%20").gsub("'", "")
      resp = Net::HTTP.get_response(URI.parse(url))
      data = resp.body
      result = JSON.parse(data)
      updateStatement = "UPDATE " + tableName + " SET trailer_link='" + sanitize((result[0] ? result[0]["code"].match(/src="(.+?)"/)[1] : ""), "string") + "' WHERE title='" + sanitize(title[0], "string") + "'"
      $con.query(updateStatement)
   rescue StandardError
      puts "Error retreiving a trailer..."
   end
end

def getIMDB(tableName)
   result_set = $con.query("SELECT `imdb_id` FROM " + tableName)
   n_rows = result_set.num_rows
   n_rows.times do |index|
      id = result_set.fetch_row
      if index != -1 && id[0].to_s != "-1"
         setIMDBValues(id[0], tableName)
      end
   end
   puts "Added IMDB info..."

end

def setIMDBValues(id, tableName)
   begin
      url = "http://mymovieapi.com/?id=tt#{id}&type=json&plot=full&episode=1&lang=en-US&aka=full&release=full&business=1&tech=1"
      resp = Net::HTTP.get_response(URI.parse(url))
      data = resp.body
      result = JSON.parse(data)
      updateStatement = "UPDATE " + tableName + " SET " + getIMDBValues(result) + " WHERE imdb_id=" + id
      puts "Updating IMDB values for " + id
      $con.query(updateStatement)
   rescue StandardError => err
      $updateMessage += "Setting IMDB Values Error Encountered... \n"
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



def getReviews(rotten_id)
   result_set = $con.query("SELECT `rotten_id` FROM Movies")
   n_rows = result_set.num_rows

   n_rows.times do |index|
      id = result_set.fetch_row
      if index != -1 && id[0].to_s != "-1"
         setReview(id[0])
      end
   end
   puts "Added Reviews..."
end

def setReview(rotten_id)
   url = "http://api.rottentomatoes.com/api/public/v1.0/movies/#{rotten_id}/reviews.json?apikey=#{$api_key}"
   resp = Net::HTTP.get_response(URI.parse(url))
   data = resp.body
   result = JSON.parse(data)
   result_set = $con.query("SELECT * FROM Reviews WHERE rotten_id = #{rotten_id}")
   if defined? result_set.fetch_row.length
      result["reviews"].each do |item|
         updateStatement = "UPDATE Reviews SET " + getReviewValues(item, "update", rotten_id) + " WHERE review_id='" + getReviewID(rotten_id, item) + "'"
         $con.query(updateStatement)
         puts "Updated Review values for #{rotten_id}"
      end
   else
      result["reviews"].each do |item|
         createStatement = "INSERT INTO Reviews(review_id, critic, rotten_id, date, freshness, original_score, publication, quote, full_review_link)
                           VALUES(" + getReviewValues(item, "create", rotten_id) + ");"
         
         $con.query(createStatement)
         $updateMessage += "Created Reviews for #{rotten_id}\n"
         puts "Created Reviews for #{rotten_id}"
      end
   end
end

def getReviewValues (item, type, rotten_id)
   full_review_link = "NA"
   if(item["links"])
      if(item["links"]["review"])
         full_review_link = ["links"][0]["review"]
      end
   end
   return (type == "update" ? "review_id=" : "") + "'" + getReviewID(rotten_id, item) + "'," + 
         (type == "update" ? "critic=" : "") + "'" + sanitize(item["critic"], "string") + "'," +
         (type == "update" ? "rotten_id=" : "") + rotten_id + "," +
         (type == "update" ? "date=" : "") + item["date"] + "," +
         (type == "update" ? "freshness=" : "") + "'" + sanitize(item["freshness"], "string") + "'," +
         (type == "update" ? "original_score=" : "") + "'" + sanitize(item["original_score"], "string") + "'," +
         (type == "update" ? "publication=" : "") + "'" + sanitize(item["publication"], "string") + "'," +
         (type == "update" ? "quote=" : "") + "'" + sanitize(item["quote"], "string") + "'," +
         (type == "update" ? "full_review_link=" : "") + "'" + sanitize(full_review_link, "string") + "'"
end

def getReviewID (rotten_id, item)
   return review_id = (rotten_id + item["critic"] + item["publication"]).gsub(" ","_").gsub("'","")
end

def setUpdate ()
   statement = "INSERT INTO UpdateRecords(message) VALUES ('" + sanitize($updateMessage, "string") + "');"
   $con.query(statement)
end

main()
