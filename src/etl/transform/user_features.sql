drop materialized view if exists user_features;

create materialized view user_features as
with
tweet_stats as (
select
    (raw_data->'user'->>'id') as user_id,
    count(*)::int as tweet_count,
    -- SPOT 1.0 features
    avg(case when tweet_text ~* 'http' then 1 else 0 end) as urls_per_tweet,
    avg(length(regexp_replace(tweet_text, '[^#]', '', 'g'))) as hashtags_per_tweet,
    avg(length(regexp_replace(tweet_text, '[^@]', '', 'g'))) as mentions_per_tweet,
    avg(case when tweet_text like 'RT %' then 1 else 0 end) as retweet_rate,
    avg(case when tweet_text not like 'RT %' and tweet_text like '@%' then 1 else 0 end) as reply_rate,
    count(distinct coalesce(
        nullif(trim(substring(raw_data->>'source' from '<a[^>]*>([^<<]*)</a>')), ''),
        raw_data->>'source'
    )) as unique_sources,

    -- bot / suspicious sources
    avg(case when coalesce(
        nullif(trim(substring(raw_data->>'source' from '<a[^>]*>([^<<]*)</a>')), ''),
        raw_data->>'source'
    ) ~* 'bot|retweet|cluster|deck |autotc|prometheus|fifa300|fcbarcelona|tweetfrommethode|smmplanner|twittbot|mkulima|b0tdaisy|juvebot|mvretweet|triendbot|crytonews|johnfalatjr|group ideapromo|botanalytic|node red|renile|twittnuker|follower20|winmines|dogtrack|pintadebot|automating rt|beer o''clock|testgb|bothadashot|twit bottt|alex retweet|rageoffifa|carreg|sup3reg|rtbteo|twopcharts|hamoooooon|distraido_|bustopsng|skysportrt|pbbsdlic|pasadas|passdd|twappdamiano|nouveau diplomix|twren\.ch|tujye|integromat|tootle|ripl|news deduper|reporteni|dfs'
        then 1 else 0 end) as bot_source_ratio
from
    tweets
group by
    user_id
)
select
    u.user_id,
    u.screen_name,
    u.name,

    -- Raw counts (drop these at modelling time if you want)
    u.followers_count,
    u.friends_count,
    u.listed_count,
    u.favourites_count,
    u.statuses_count,
    u.verified,
    u.default_profile,
    u.default_profile_image,
    u.created_at,
    u.last_tweet_at,
    u.first_seen_at,
    u.last_seen_at,
    u.url            as profile_url,
    u.description,

    -- Tweet content stats
    coalesce(t.tweet_count, 0)        as tweet_count,
    t.urls_per_tweet,
    t.hashtags_per_tweet,
    t.mentions_per_tweet,
    t.retweet_rate,
    t.reply_rate,
    t.unique_sources,
    t.bot_source_ratio,


    -- SPOT
    (u.followers_count::float / nullif(u.friends_count, 0))
        as followers_friends_ratio,
    (u.followers_count::float / (u.followers_count + u.friends_count + 1))
        as reputation,
    (u.favourites_count::float / nullif(u.statuses_count, 0))
        as favourites_per_status,
    (u.listed_count::float / nullif(u.followers_count, 0))
        as listed_per_follower,

    -- Temporal features
    extract(day from (timestamp '2018-06-17' - u.created_at))
        as account_age_days,
    extract(day from (timestamp '2018-06-17' - u.last_tweet_at))
        as days_since_last_tweet,
    extract(day from (u.last_seen_at - u.first_seen_at))
        as observation_span_days,

    -- Frequencies
    coalesce(t.tweet_count, 0)::float
        / greatest(extract(day from (timestamp '2018-06-17' - u.created_at)), 1)
        as tweet_frequency,
    coalesce(t.tweet_count, 0)::float
        / greatest(extract(day from (u.last_seen_at - u.first_seen_at)), 1)
        as tweets_per_day_in_dataset,

    -- Textual / profile features
    coalesce(length(u.description), 0)                  as description_length,
    case when u.description is not null then 1 else 0 end as has_description,
    case when u.url       is not null then 1 else 0 end   as has_url,
    case when u.location  is not null then 1 else 0 end   as has_location,
    length(u.screen_name)                                 as screen_name_length,
    case when u.screen_name ~ '\d' then 1 else 0 end      as screen_name_has_digits

from users u
left join tweet_stats t
    on u.user_id = t.user_id::bigint;

create unique index idx_user_features_user_id on user_features (user_id);
