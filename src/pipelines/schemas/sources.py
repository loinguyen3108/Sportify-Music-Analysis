from pyspark.sql.types import ArrayType, BooleanType, IntegerType, LongType, ShortType, \
    StringType, StructField, StructType, TimestampType

ALBUM = StructType([
    StructField(name='album_id', dataType=StringType()),
    StructField(name='created_at', dataType=TimestampType()),
    StructField(name='updated_at', dataType=TimestampType()),
    StructField(name='available_markets', dataType=ArrayType(StringType())),
    StructField(name='url', dataType=StringType()),
    StructField(name='cover_image', dataType=StringType()),
    StructField(name='name', dataType=StringType()),
    StructField(name='release_date', dataType=StringType()),
    StructField(name='release_date_precision', dataType=StringType()),
    StructField(name='restrictions', dataType=ArrayType(StringType())),
    StructField(name='label', dataType=StringType()),
    StructField(name='popularity', dataType=ShortType()),
    StructField(name='album_type', dataType=StringType())
])

ARTIST = StructType([
    StructField(name='artist_id', dataType=StringType()),
    StructField(name='created_at', dataType=TimestampType()),
    StructField(name='updated_at', dataType=TimestampType()),
    StructField(name='url', dataType=StringType()),
    StructField(name='followers_count', dataType=IntegerType()),
    StructField(name='genres', dataType=ArrayType(StringType())),
    StructField(name='image', dataType=StringType()),
    StructField(name='name', dataType=StringType()),
    StructField(name='popularity', dataType=ShortType()),
    StructField(name='monthly_listeners', dataType=LongType())
])

ARTIST_ALBUM = StructType([
    StructField(name='artist_id', dataType=StringType()),
    StructField(name='album_id', dataType=StringType()),
    StructField(name='created_at', dataType=TimestampType()),
    StructField(name='updated_at', dataType=TimestampType())
])

ARTIST_TRACK = StructType([
    StructField(name='artist_id', dataType=StringType()),
    StructField(name='track_id', dataType=StringType()),
    StructField(name='created_at', dataType=TimestampType()),
    StructField(name='updated_at', dataType=TimestampType())
])

PLAYLIST = StructType([
    StructField(name='playlist_id', dataType=StringType()),
    StructField(name='created_at', dataType=TimestampType()),
    StructField(name='updated_at', dataType=TimestampType()),
    StructField(name='collaborative', dataType=BooleanType()),
    StructField(name='description', dataType=StringType()),
    StructField(name='url', dataType=StringType()),
    StructField(name='followers_count', dataType=IntegerType()),
    StructField(name='cover_image', dataType=StringType()),
    StructField(name='name', dataType=StringType()),
    StructField(name='user_id', dataType=StringType()),
    StructField(name='public', dataType=BooleanType()),
    StructField(name='snapshot_id', dataType=StringType()),
])

PLAYLIST_TRACK = StructType([
    StructField(name='playlist_id', dataType=StringType()),
    StructField(name='track_id', dataType=StringType()),
    StructField(name='added_at', dataType=StringType()),
    StructField(name='added_by', dataType=StringType()),
    StructField(name='url', dataType=StringType()),
    StructField(name='created_at', dataType=TimestampType()),
    StructField(name='updated_at', dataType=TimestampType())
])

TRACK = StructType([
    StructField(name='track_id', dataType=StringType()),
    StructField(name='created_at', dataType=TimestampType()),
    StructField(name='updated_at', dataType=TimestampType()),
    StructField(name='album_id', dataType=StringType()),
    StructField(name='available_markets', dataType=ArrayType(StringType())),
    StructField(name='disc_number', dataType=IntegerType()),
    StructField(name='duration_ms', dataType=IntegerType()),
    StructField(name='explicit', dataType=BooleanType()),
    StructField(name='url', dataType=StringType()),
    StructField(name='name', dataType=StringType()),
    StructField(name='popularity', dataType=ShortType()),
    StructField(name='restrictions', dataType=ArrayType(StringType())),
    StructField(name='track_number', dataType=IntegerType()),
    StructField(name='plays_count', dataType=LongType())
])

USER = StructType([
    StructField(name='user_id', dataType=StringType()),
    StructField(name='created_at', dataType=TimestampType()),
    StructField(name='updated_at', dataType=TimestampType()),
    StructField(name='url', dataType=StringType()),
    StructField(name='image', dataType=StringType()),
    StructField(name='name', dataType=StringType())
])
