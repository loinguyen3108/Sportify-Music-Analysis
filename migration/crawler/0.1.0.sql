-- Schema: public
-- Table: album
CREATE TABLE IF NOT EXISTS public.album (
    album_id TEXT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    album_type TEXT,
    available_markets TEXT[],
    url TEXT NOT NULL,
    cover_image TEXT,
    name TEXT NOT NULL,
    release_date TEXT,
    release_date_precision TEXT,
    restrictions TEXT[],
    label TEXT,
    popularity SMALLINT
  );

-- Table: artist
CREATE TABLE IF NOT EXISTS public.artist (
    artist_id TEXT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    url TEXT NOT NULL,
    followers_count INTEGER,
    genres TEXT[],
    image TEXT,
    NAME TEXT,
    popularity SMALLINT,
    monthly_listeners BIGINT
  );

-- Table: playlist
CREATE TABLE IF NOT EXISTS public.playlist (
    playlist_id TEXT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    collaborative BOOLEAN NOT NULL,
    description TEXT,
    url TEXT NOT NULL,
    followers_count INTEGER,
    cover_image TEXT,
    name TEXT NOT NULL,
    user_id TEXT NOT NULL,
    public BOOLEAN NOT NULL,
    snapshot_id TEXT NOT NULL
  );

-- Table: track
CREATE TABLE IF NOT EXISTS public.track (
    track_id TEXT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    album_id TEXT NOT NULL,
    available_markets TEXT[],
    disc_number INTEGER,
    duration_ms INTEGER NOT NULL,
    explicit BOOLEAN,
    url TEXT NOT NULL,
    NAME TEXT NOT NULL,
    popularity SMALLINT,
    restrictions TEXT[],
    track_number INTEGER NOT NULL,
    plays_count BIGINT
  );

-- Table: playlist_track
CREATE TABLE IF NOT EXISTS public.playlist_track (
    playlist_id TEXT NOT NULL,
    track_id TEXT NOT NULL,
    added_at TEXT NOT NULL,
    added_by TEXT,
    url TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (playlist_id, track_id)
  );

CREATE INDEX IF NOT EXISTS idx_playlist_track_track_id ON public.playlist_track (track_id);

-- Table: artist_album
CREATE TABLE IF NOT EXISTS public.artist_album (
    artist_id TEXT NOT NULL,
    album_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (artist_id, album_id)
  );

-- Table: artist_track
CREATE TABLE IF NOT EXISTS public.artist_track (
    artist_id TEXT NOT NULL,
    track_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (artist_id, track_id)
  );

-- Table: user
CREATE TABLE IF NOT EXISTS public.user (
    user_id TEXT NOT NULL PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    url TEXT NOT NULL,
    image TEXT,
    NAME TEXT
  );

-- Table: crawler_tracking
CREATE TABLE IF NOT EXISTS public.crawler_tracking (
    function_name TEXT NOT NULL,
    main_arg_name TEXT NOT NULL,
    main_arg_value TEXT NOT NULL,
    tracked_at DATE NOT NULL,
    PRIMARY KEY (
      function_name,
      main_arg_name,
      main_arg_value,
      tracked_at
    )
  );