DROP TABLE IF EXISTS public.staging_cases;

CREATE TABLE public.staging_cases (
    dateRep DATE NULL,
    day INTEGER NULL,
    month INTEGER NULL,
    year INTEGER NULL,
    cases INTEGER NULL,
    deaths INTEGER NULL,
    countriesAndTerritories TEXT NULL,
    geoId TEXT NULL,
    countryterritoryCode TEXT NULL,
    popData2018 REAL NULL
);

DROP TABLE IF EXISTS public.tweets;

CREATE TABLE public.tweets (
    id BIGINT IDENTITY(0, 1) PRIMARY KEY,
    tweet_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL SORTKEY,
    full_text VARCHAR(1500) NULL,
    created_by BIGINT NOT NULL,
    language TEXT NULL,
    retweet_count INTEGER NULL,
    favorite_count INTEGER NULL,
    in_reply_to_status_id BIGINT NULL,
    in_reply_to_user_id BIGINT NULL
);

DROP TABLE IF EXISTS public.users;

CREATE TABLE IF NOT EXISTS public.users (
    id BIGINT IDENTITY(0, 1) PRIMARY KEY,
    user_id BIGINT NOT NULL,
    name TEXT NULL,
    screen_name TEXT NULL,
    location VARCHAR(500) NULL,
    description VARCHAR(750) NULL,
    verified BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    followers_count INTEGER NULL,
    friends_count INTEGER NULL,
    statuses_count INTEGER NULL
);

DROP TABLE IF EXISTS public.covid19_stats;

CREATE TABLE IF NOT EXISTS public.covid19_stats (
    id BIGINT IDENTITY(0, 1) PRIMARY KEY,
    country TEXT NOT NULL,
    day DATE NOT NULL SORTKEY,
    cases INTEGER NULL,
    deaths INTEGER NULL
);

DROP TABLE IF EXISTS public.country_stats;

CREATE TABLE IF NOT EXISTS public.country_stats (
    id BIGINT IDENTITY(0, 1) PRIMARY KEY,
    country TEXT NOT NULL,
    population INTEGER NULL
);
