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

con = Mysql.new 'movieinstance.cnybahrpes0y.us-west-2.rds.amazonaws.com', 'root', 'welcome08', 'myMovies'

create_table(con, "Movies")

con.close()