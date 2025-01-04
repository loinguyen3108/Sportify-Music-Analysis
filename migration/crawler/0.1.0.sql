-- Schema: public
-- Table: album
CREATE TABLE IF NOT EXISTS public.album (
    album_id text NOT NULL primary key,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now(),
    album_type text,
    available_markets text [],
    url text NOT NULL,
    cover_image text,
    name text NOT NULL,
    release_date text,
    release_date_precision text,
    restrictions text [],
    label text,
    popularity SMALLINT
);
-- Table: artist
CREATE TABLE IF NOT EXISTS public.artist (
    artist_id text NOT NULL primary key,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now(),
    url text NOT NULL,
    followers_count INTEGER,
    genres text [],
    image text,
    name text,
    popularity SMALLINT,
    monthly_listeners bigint
);
-- Table: playlist
CREATE TABLE IF NOT EXISTS public.playlist (
    playlist_id text NOT NULL primary key,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now(),
    collaborative BOOLEAN NOT NULL,
    description text,
    url text NOT NULL,
    followers_count INTEGER,
    cover_image text,
    name text NOT NULL,
    user_id text NOT NULL,
    public BOOLEAN NOT NULL,
    snapshot_id text NOT NULL
);
-- Table: track
CREATE TABLE IF NOT EXISTS public.track (
    track_id text NOT NULL primary key,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now(),
    album_id text NOT NULL,
    available_markets text [],
    disc_number INTEGER,
    duration_ms INTEGER NOT NULL,
    explicit BOOLEAN,
    url text NOT NULL,
    name text NOT NULL,
    popularity SMALLINT,
    restrictions text [],
    track_number INTEGER NOT NULL,
    plays_count bigint
);
-- Table: playlist_track
CREATE TABLE IF NOT EXISTS public.playlist_track (
    playlist_id text NOT NULL,
    track_id text NOT NULL,
    added_at text NOT NULL,
    added_by text NOT NULL,
    url text,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    primary key (
        playlist_id,
        track_id
    )
);
CREATE INDEX IF NOT EXISTS idx_playlist_track_track_id
ON public.playlist_track (track_id);
-- Table: artist_album
CREATE TABLE IF NOT EXISTS public.artist_album (
    artist_id text NOT NULL,
    album_id text NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    primary key (
        artist_id,
        album_id
    )
);
-- Table: artist_track
CREATE TABLE IF NOT EXISTS public.artist_track (
    artist_id text NOT NULL,
    track_id text NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now(),
    primary key (
        artist_id,
        track_id
    )
);
-- Table: user
CREATE TABLE IF NOT EXISTS public.user (
    user_id text NOT NULL primary key,
    created_at timestamp NOT NULL DEFAULT now(),
    updated_at timestamp NOT NULL DEFAULT now(),
    url text NOT NULL,
    image text,
    name text
);
-- Table: crawler_tracking
CREATE TABLE IF NOT EXISTS public.crawler_tracking (
    function_name text NOT NULL,
    main_arg_name text NOT NULL,
    main_arg_value text NOT NULl,
    tracked_at DATE NOT NULL,
    primary key (
        function_name,
        main_arg_name,
        main_arg_value,
        tracked_at
    )
);
