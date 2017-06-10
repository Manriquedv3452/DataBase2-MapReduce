package insertmysql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

/**
 *
 * @author randy
 */
public class InsertMySQL {
    
    public static final String PATH_MAPREDUCE1Y2 = "/home/randy/Desktop/GitHub/DataBase2/mapReduce/resultMapReduce1.txt";
    public static final String PATH_MAPREDUCE3 = "/home/randy/Desktop/GitHub/DataBase2/mapReduce/resultMapReduce3.txt";
    public static final String PATH_MAPREDUCE4 = "/home/randy/Desktop/GitHub/DataBase2/mapReduce/resultMapReduce4.txt";
    public static final String PATH_MAPREDUCE5 = "/home/randy/Desktop/GitHub/DataBase2/mapReduce/resultMapReduce5.txt";
    public static final String PATH_MAPREDUCE6 = "/home/randy/Desktop/GitHub/DataBase2/mapReduce/resultMapReduce6.txt";
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        
        try {
            // create a mysql database connection           
            String url = "jdbc:mysql://localhost:3306/twitter";
            Connection connection = DriverManager.getConnection(url,"root","root");
            Statement statement = connection.createStatement();
            String line;
            String hashtag;
            String word;
            String total;
            String SQL;
            String totalTweets;
            String totalUsers;
            String dateHour;
            String date;
            String hour;
            String topHashtag;
            String[] tweetsInfo;
            String[] tweetsAndUsers;
            String[] wordAndTotal;
            String[] info;
            String[] dateHourTotal;
            String[] topHashtagAndTotal;
            File data;
            BufferedReader bufferedReader;
            
            //Insertar el MapReduce 1 y 2
            data = new File(PATH_MAPREDUCE1Y2);
            bufferedReader = new BufferedReader(new FileReader(data));

            while ((line = bufferedReader.readLine()) != null) {
                tweetsInfo = line.split("=");
                tweetsAndUsers = tweetsInfo[1].split(",");
                hashtag = tweetsInfo[0];
                totalTweets = tweetsAndUsers[0];
                totalUsers = tweetsAndUsers[1];
                
                SQL = "INSERT INTO hashtagsTweets VALUES ('" + hashtag + "', " + totalTweets + ", " + totalUsers + ");";
                statement.executeUpdate(SQL);
            }
            
            //Insertar el MapReduce 3
            data = new File(PATH_MAPREDUCE3);
            bufferedReader = new BufferedReader(new FileReader(data));
            
            System.out.println("Reading file...");
            while ((line = bufferedReader.readLine()) != null) {
                tweetsInfo = line.split("%");
                wordAndTotal = tweetsInfo[1].split("#");
                hashtag = tweetsInfo[0];
                int i=1;
                
                for (String wordTotal : wordAndTotal) {
                    info = wordTotal.split("=");
                    word = info[0];
                    if(i < wordAndTotal.length){
                        total = info[1];
                        SQL = "INSERT INTO topWords VALUES ('" + hashtag + "', '" + word + "', " + total + ");";
                        statement.executeUpdate(SQL);
                        //System.out.println("INSERT INTO topWords VALUES ('" + hashtag + "', '" + word + "', " + total + ");");
                    }
                    i++;
                    
                }
            }
            
            //Insertar el MapReduce 4
            data = new File(PATH_MAPREDUCE4);
            bufferedReader = new BufferedReader(new FileReader(data));
            
            System.out.println("Reading file...");
            while ((line = bufferedReader.readLine()) != null) {
                tweetsInfo = line.split("%");
                if(tweetsInfo.length > 1){
                    wordAndTotal = tweetsInfo[1].split("#");
                    hashtag = tweetsInfo[0];
                    int i=1;

                    for (String wordTotal : wordAndTotal) {
                        info = wordTotal.split("=");
                        word = info[0];
                        if(i < wordAndTotal.length){
                            total = info[1];
                            SQL = "INSERT INTO topExtraHashtags VALUES ('" + hashtag + "', '" + word + "', " + total + ");";
                            statement.executeUpdate(SQL);
                            //System.out.println("INSERT INTO topWords VALUES ('" + hashtag + "', '" + word + "', " + total + ");");
                        }
                        i++;

                    }
                }
            }
            
            //Insertar el MapReduce 5
            data = new File(PATH_MAPREDUCE5);
            bufferedReader = new BufferedReader(new FileReader(data));
            
            System.out.println("Reading file...");
            while ((line = bufferedReader.readLine()) != null) {
                tweetsInfo = line.split("%");
                if(tweetsInfo.length > 1){
                    dateHourTotal = tweetsInfo[1].split("#");
                    hashtag = tweetsInfo[0];
                    int i=1;

                    for (String pair : dateHourTotal) {
                        info = pair.split("=");
                        dateHour = info[0];
                        total = info[1];
                        if(i < dateHourTotal.length){
                            date = dateHour.split("-")[0];
                            date = date.replace("/", "-");
                            hour = dateHour.split("-")[1];
                            
                            SQL = "INSERT INTO hourDistribution VALUES ('" + hashtag + "', '" + date + "', " + hour + ", " + total +");";
                            statement.executeUpdate(SQL);
                            //System.out.println("INSERT INTO topWords VALUES ('" + hashtag + "', '" + date + "', " + hour + ", " + total +");");
                        }
                        i++;

                    }
                }
            }
            
            //Insertar el MapReduce 6
            data = new File(PATH_MAPREDUCE6);
            bufferedReader = new BufferedReader(new FileReader(data));
            
            System.out.println("Reading file...");
            while ((line = bufferedReader.readLine()) != null) {
                tweetsInfo = line.split("%");
                topHashtagAndTotal = tweetsInfo[1].split("#");
                hashtag = tweetsInfo[0];
                int i=1;
                
                for (String pair : topHashtagAndTotal) {
                    info = pair.split("=");
                    topHashtag = info[0];
                    if(i < topHashtagAndTotal.length){
                        total = info[1];
                        SQL = "INSERT INTO topHashtags VALUES ('" + hashtag + "', '" + topHashtag + "', " + total + ");";
                        statement.executeUpdate(SQL);
                        //System.out.println("INSERT INTO topWords VALUES ('" + hashtag + "', '" + word + "', " + total + ");");
                    }
                    i++;
                    
                }
            }
            
            connection.close();
            
        } catch (IOException | SQLException e) { 
            System.err.println("Got an exception! "); 
            System.err.println(e.getMessage()); 
        }
    }
}