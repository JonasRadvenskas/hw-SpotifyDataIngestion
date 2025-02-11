CREATE TABLE IF NOT EXISTS public."Artists"
(
	 "ArtistId" character(22) COLLATE pg_catalog."default" NOT NULL,
	 "ArtistName" character varying(500) COLLATE pg_catalog."default" NOT NULL,
	 "Followers" integer NULL,
	 "Popularity" integer NULL
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Artists" OWNER to postgres;

CREATE INDEX IF NOT EXISTS ArtistId_Artists ON "Artists" ("ArtistId");

------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."ArtistGenres"
(
	"ArtistGenreRelId" integer NOT NULL GENERATED ALWAYS AS IDENTITY (INCREMENT 1 START 1),
	"ArtistId" character(22) COLLATE pg_catalog."default" NOT NULL,
	"GenreName" character varying(255) COLLATE pg_catalog."default" NOT NULL
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."ArtistGenres" OWNER to postgres;

CREATE INDEX IF NOT EXISTS ArtistId_Genres ON "ArtistGenres" ("ArtistId");

------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."Tracks"
(
	"TrackId" character(22) COLLATE pg_catalog."default" NOT NULL,
	"TrackName" character varying(2000) COLLATE pg_catalog."default" NOT NULL,
	"Popularity" integer NULL,
	"DurationMS" bigint NULL,
	"Explicit" integer NULL,
	"ReleaseYear" smallint NULL,
	"ReleaseMonth" smallint NULL,
	"ReleaseDay" smallint NULL,
	"Danceability" character varying(50) COLLATE pg_catalog."default" NOT NULL,
	"Energy" numeric(18,10) NOT NULL,
	"Key" integer NOT NULL,
	"Loudness" numeric(18,10) NOT NULL,
	"Mode" integer NOT NULL,
	"Speechiness" numeric(18,10) NOT NULL,
	"Acousticness" numeric(18,10) NOT NULL,
	"Instrumentalness" numeric(18,10) NOT NULL,
	"Liveness" numeric(18,10) NOT NULL,
	"Valence" numeric(18,10) NOT NULL,
	"Tempo" numeric(18,10) NOT NULL,
	"TimeSignature" integer NOT NULL
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."Tracks" OWNER to postgres;

CREATE INDEX IF NOT EXISTS TrackId_Tracks ON "Tracks" ("TrackId");

------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS public."TrackArtists"
(
	"TrackArtistRelId" integer NOT NULL GENERATED ALWAYS AS IDENTITY (INCREMENT 1 START 1),
	"TrackId" character(22) COLLATE pg_catalog."default" NOT NULL,
	"ArtistId" character(22) COLLATE pg_catalog."default" NOT NULL
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."TrackArtists" OWNER to postgres;

CREATE INDEX IF NOT EXISTS TrackId_TrackArtists ON "TrackArtists" ("TrackId");

------------------------------------------------------------------
