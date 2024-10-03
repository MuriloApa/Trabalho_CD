create table spotify(
    dates DATE,
    ids TEXT,
    names TEXT,
    monthly_listeners DOUBLE PRECISION,
    popularity INT,
    followers INT,
    genres TEXT,
    first_release INT,
    last_release INT,
    num_releases INT,
    num_tracks INT,
    playlists_found TEXT,
    feat_track_ids TEXT

);

COPY spotify(dates,ids,names,monthly_listeners,popularity,followers,genres,first_release,last_release,num_releases,num_tracks,playlists_found,feat_track_ids)
FROM '/dados/dataset.csv'
DELIMITER ','
CSV HEADER;