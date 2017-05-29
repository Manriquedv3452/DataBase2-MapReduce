/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterconnection;

/**
 *
 * @author manriquedv
 */


import java.io.IOException;
import java.lang.String;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

public class SimpleStream1 {

    public static void main(String[] args) {
        
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();

        
        Path file = Paths.get("tweets.txt");
                
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("KvtnQ2stn6lDaqgfOEaBF4wK8");
        cb.setOAuthConsumerSecret("u6f1YYzxh8enWnMmpZ1aw7VlyS8SSRfBaQaMk8831QQWqQkGE6");
        cb.setOAuthAccessToken("267813265-qfwXUvIUH2G6xFSIUl0GCY8Rmu65ebmBbC4PqbzF");
        cb.setOAuthAccessTokenSecret("3Yk3636OZrHjv3ayHHTO9rDCTejEvYp0ypVIMy1vP9Kea");

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        StatusListener listener = new StatusListener() {

            @Override
            public void onException(Exception arg0) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice arg0) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onScrubGeo(long arg0, long arg1) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onStatus(Status status) {
                
                if (!status.isRetweet()){
                    User user = status.getUser();

                    // gets Username
                 
                    String username = status.getUser().getScreenName();
                    String tweetInfo = username + "%" + dateTimeFormatter.format(now) + "%";
                    HashtagEntity[] hashtags = status.getHashtagEntities();

                    for (int i = 0; i < status.getHashtagEntities().length; i++){
                        tweetInfo += "#" + hashtags[i].getText();
                    }
                    tweetInfo += "%";
                    
                    String tweetContent = status.getText();
                    tweetContent = tweetContent.replace("%", "");
                    tweetContent = tweetContent.replaceAll("[^\\x00-\\x7f]", "");
                    tweetContent = tweetContent.replaceAll("\n", "\b");
                    
 
                    
                    
                    List<String> lines = Arrays.asList(tweetInfo + tweetContent);
                    try {
                        Files.write(file, lines, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
                    } catch (IOException ex) {
                    }
                    
 
                    
                }
                
                
            }

            @Override
            public void onTrackLimitationNotice(int arg0) {
                // TODO Auto-generated method stub

            }

            @Override
            public void onStallWarning(StallWarning sw) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
            

        };
        FilterQuery filterQuery = new FilterQuery();
    
        String keywords[] = {"#2030NOW", 
                             "#women", 
                             "#costarica", 
                             "#puravida", 
                             "#MakeAmericaGreatAgain", 
                             "#Trumprussia",
                             "#RecycleReuse",
                             "#TraficoCR"};

        filterQuery.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(filterQuery);  
        

    }
}