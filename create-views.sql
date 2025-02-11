DROP VIEW public."vw_TakeTrack";

CREATE VIEW public."vw_TakeTrack"
AS
SELECT 
	 t."TrackId"
    ,t."TrackName"
    ,t."Popularity"
    ,t."Energy"
    ,t."Danceability"
	,SUM(coalesce(a."Followers", 0)) AS "Followers"
FROM public."Tracks" AS t
	LEFT JOIN public."TrackArtists" AS ta
		ON t."TrackId" = ta."TrackId"
	LEFT JOIN public."Artists" AS a
		ON ta."ArtistId" = a."ArtistId"
GROUP BY t."TrackId"
    ,t."TrackName"
    ,t."Popularity"
    ,t."Energy"
    ,t."Danceability";

ALTER TABLE public."vw_TakeTrack"
    OWNER TO postgres;

-------------------------------------------------------

DROP VIEW public."vw_FollowedTrack";

CREATE VIEW public."vw_FollowedTrack"
AS
SELECT DISTINCT
	 t."TrackId"
	,t."TrackName"
	,t."Popularity"
	,t."DurationMS"
	,t."Explicit"
	,t."ReleaseYear"
	,t."ReleaseMonth"
	,t."ReleaseDay"
	,t."Danceability"
	,t."Energy"
	,t."Key"
	,t."Loudness"
	,t."Mode"
	,t."Speechiness"
	,t."Acousticness"
	,t."Instrumentalness"
	,t."Liveness"
	,t."Valence"
	,t."Tempo"
	,t."TimeSignature"
FROM public."Tracks" AS t
	INNER JOIN public."TrackArtists" AS ta
		ON t."TrackId" = ta."TrackId"
	LEFT JOIN public."Artists" AS a
		ON ta."ArtistId" = a."ArtistId"
WHERE coalesce(a."Followers", 0) > 0;

ALTER TABLE public."vw_FollowedTrack"
    OWNER TO postgres;

-------------------------------------------------------

DROP VIEW public."vw_AnnualEnergisingTrack";

CREATE VIEW public."vw_AnnualEnergisingTrack"
AS
SELECT 
	 "TrackName"
	,"ReleaseYear"
	,"Energy"
FROM (
	SELECT
		 "TrackName"
		,"ReleaseYear"
		,"Energy"
		,row_number() OVER (PARTITION BY "ReleaseYear" ORDER BY "Energy" DESC) AS "PosId"
	FROM public."Tracks"
) AS ranked
WHERE "PosId" = 1
ORDER BY "ReleaseYear" DESC;

ALTER TABLE public."vw_AnnualEnergisingTrack"
    OWNER TO postgres;

-------------------------------------------------------
