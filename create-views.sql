CREATE OR REPLACE VIEW public."vw_TakeTrack"
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








    