-- TWEETS and USERS tables

CREATE TABLE IF NOT EXISTS tweets (
    id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    tweet_text TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS users (
    user_id          BIGINT PRIMARY KEY,
    screen_name      TEXT,
    name             TEXT,
    description      TEXT,
    location         TEXT,
    url              TEXT,
    followers_count  INTEGER,
    friends_count    INTEGER,
    listed_count     INTEGER,
    favourites_count INTEGER,
    statuses_count   INTEGER,
    verified         BOOLEAN,
    created_at       TIMESTAMP,          -- account creation date
    profile_image_url TEXT,
    default_profile  BOOLEAN,
    default_profile_image BOOLEAN,
    raw_user         JSONB,              -- full user object from Twitter
    tweet_count      INTEGER,            -- how many tweets we have for this user
    last_tweet_at    TIMESTAMP,          -- date of most recent tweet we captured
    first_seen_at    TIMESTAMP,          -- first time this user appeared in our data
    last_seen_at     TIMESTAMP           -- last time this user appeared in our data
);

-- INDEXES

CREATE INDEX IF NOT EXISTS idx_tweets_raw_data ON tweets USING GIN (raw_data);
CREATE INDEX IF NOT EXISTS idx_users_screen_name ON users (screen_name);
CREATE INDEX IF NOT EXISTS idx_users_last_tweet ON users (last_tweet_at DESC);

CREATE INDEX IF NOT EXISTS idx_tweets_user_id
    ON tweets (((raw_data->'user'->>'id')::BIGINT));

CREATE INDEX IF NOT EXISTS idx_tweets_user_date
    ON tweets (((raw_data->'user'->>'id')::BIGINT), created_at DESC);
