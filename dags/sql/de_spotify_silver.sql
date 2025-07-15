{% set medians_modes = ti.xcom_pull(task_ids='compute_medians_modes') %}
{% set medians = medians_modes['medians'] %}
{% set modes = medians_modes['modes'] %}

CREATE TABLE IF NOT EXISTS spotify_tracks_silver LIKE spotify_tracks;

INSERT INTO spotify_tracks_silver (
    `index`, track_id, artists, album_name, track_name, popularity, duration_ms, explicit,
    danceability, energy, `key`, loudness, mode, speechiness, acousticness, instrumentalness,
    liveness, valence, tempo, time_signature, track_genre, ingestion_timestamp, source_identifier,
    batch_identifier, created_at, updated_at
)
SELECT
    `index`,
    track_id,
    COALESCE(artists, '{{ modes['artists'] }}') AS artists,
    COALESCE(album_name, '{{ modes['album_name'] }}') AS album_name,
    COALESCE(track_name, '{{ modes['track_name'] }}') AS track_name,
    LEAST(GREATEST(COALESCE(popularity, {{ medians['popularity'] }}), 0), 100) AS popularity,
    COALESCE(duration_ms, {{ medians['duration_ms'] }}) AS duration_ms,
    explicit,
    LEAST(GREATEST(COALESCE(danceability, {{ medians['danceability'] }}), 0.0), 1.0) AS danceability,
    LEAST(GREATEST(COALESCE(energy, {{ medians['energy'] }}), 0.0), 1.0) AS energy,
    `key`,
    COALESCE(loudness, {{ medians['loudness'] }}) AS loudness,
    mode,
    LEAST(GREATEST(COALESCE(speechiness, {{ medians['speechiness'] }}), 0.0), 1.0) AS speechiness,
    LEAST(GREATEST(COALESCE(acousticness, {{ medians['acousticness'] }}), 0.0), 1.0) AS acousticness,
    LEAST(GREATEST(COALESCE(instrumentalness, {{ medians['instrumentalness'] }}), 0.0), 1.0) AS instrumentalness,
    LEAST(GREATEST(COALESCE(liveness, {{ medians['liveness'] }}), 0.0), 1.0) AS liveness,
    LEAST(GREATEST(COALESCE(valence, {{ medians['valence'] }}), 0.0), 1.0) AS valence,
    COALESCE(tempo, {{ medians['tempo'] }}) AS tempo,
    time_signature,
    COALESCE(track_genre, '{{ modes['track_genre'] }}') AS track_genre,
    ingestion_timestamp,
    source_identifier,
    batch_identifier,
    created_at,
    updated_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY `index`) AS rn
    FROM spotify_tracks
) t
WHERE rn = 1; 