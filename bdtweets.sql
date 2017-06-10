create database twitter;

use twitter;

-- mr 1 y 2
create table hashtagsTweets(
	hashtag varchar(128) not null,
    users int not null,
    tweets int not null,
    constraint primary key pk_tweets (hashtag)
);

-- mr 3
create table topWords(
	hashtag varchar(128) not null,
	word varchar(128) not null,
    total int not null,
    constraint primary key pk_tweets_hour(hashtag, word),
    constraint foreign key fk_tweets_hour(hashtag) references hashtagsTweets(hashtag)
);

-- mr 6
create table topHashtags(
	hashtag varchar(128) not null,
    topHashtag varchar(128) not null,
    total int not null,
    constraint primary key pk_top_hashtags(hashtag, topHashtag),
    constraint foreign key fk_top_hashtags(hashtag) references hashtagsTweets(hashtag),
    constraint foreign key fk_top_hashtags(topHashtag) references hashtagsTweets(hashtag)
);

-- mr 4
create table topExtraHashtags(
    hashtag varchar(128) not null,
	extraHashtag varchar(128) not null,
    total int not null,
    constraint primary key pk_top_extra_hashtags(hashtag, extraHashtag),
    constraint foreign key fk_top_extra_hashtags(hashtag) references hashtagsTweets(hashtag)
);

-- mr 5
create table hourDistribution(
	hashtag varchar(128) not null,
    dateTweet date not null,
    hourTweet int not null,
    total int not null,
    constraint primary key pk_hour_distribuion(hashtag, dateTweet, hourTweet, total),
    constraint foreign key fk_hour_distribution(hashtag) references hashtagsTweets(hashtag)
);
