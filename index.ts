import * as fs from 'fs';
import { parse } from 'csv-parse';
import { Client } from 'pg';




let tracksFilePath = 'data/tracks.csv';
let artistsFilePath = 'data/artists.csv';

//id,followers,genres,name,popularity
type typeArtist = {    
    id: String,
    followers: number,
    genres: String[],
    name: String,
    popularity: number
}

//id,name,popularity,duration_ms,explicit,artists,id_artists,release_date,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,time_signature
type typeTrack = {
    id: String,
    name: String,
    popularity: number,
    duration_ms: number,
    explicit: number,
    artists: String[],
    id_artists: String[],
    //release_date: String[],
    release_year: number,
    release_month: number,
    release_day: number,
    danceability: number,
    energy: number,
    key: number,
    loudness: number,
    mode: number,
    speechiness: number,
    acousticness: number,
    instrumentalness: number,
    liveness: number,
    valence: number,
    tempo: number,
    time_signature: number
}

var fileContent = fs.readFileSync(tracksFilePath, { encoding: 'utf-8' });

// Load tracks dataset
parse(
    fileContent,
    {
        delimiter: ',',
        columns: true,  // first line as a header and starts parsing from the 2nd line
        cast: (columnValue, context) => {
            // data type conversion
            if (   context.column === 'duration_ms'
                || context.column === 'popularity'
                || context.column === 'explicit'
                || context.column === 'key'
                || context.column === 'mode'
                || context.column === 'time_signature'
            ) {
                return parseInt(columnValue);
            }

            // Transform track danceability into string values
            if (context.column === 'danceability') {
                let num = parseFloat(columnValue);

                if (num > 0.6) {
                    return 'High';
                } else if (num >= 0.5) {
                    return 'Medium';
                }

                return 'Low';
            }

            // data type conversion
            if (   context.column === 'energy'
                || context.column === 'loudness'
                || context.column === 'speechiness'
                || context.column === 'acousticness'
                || context.column === 'instrumentalness'
                || context.column === 'liveness'
                || context.column === 'valence'
                || context.column === 'tempo'
            ) {
                return parseFloat(columnValue);
            }

            // data cleanup
            if (context.column === 'artists'
                || context.column === 'id_artists'
            ) {
                return columnValue.replace(/\]|\[|\'/g,'').split(',');
            }

            // Explode track release date into separate columns: year, month, day
            if (context.column === 'release_date') {
                return columnValue.split('-');
            }
        
            return columnValue;
        },
        on_record: (line, context) => {
            // Ignore the tracks that have no name
            if (!line.name) {
                return;
            }

            // Ignore the tracks shorter than 1 minute
            if (line.duration_ms / 1000.0 / 60.0 < 1.0) {
                return;
            }

            // Explode track release date into separate columns: year, month, day
            return {
                'id': line.id,
                'name': line.name,
                'popularity': line.popularity,
                'duration_ms': line.duration_ms,
                'explicit': line.explicit,
                'artists': line.artists,
                'id_artists': line.id_artists,
                'release_year': line.release_date[0],
                'release_month': line.release_date[1],
                'release_day': line.release_date[2],
                'danceability': line.danceability,
                'energy': line.energy,
                'key': line.key,
                'loudness': line.loudness,
                'mode': line.mode,
                'speechiness': line.speechiness,
                'acousticness': line.acousticness,
                'instrumentalness': line.instrumentalness,
                'liveness': line.liveness,
                'valence': line.valence,
                'tempo': line.tempo,
                'time_signature': line.time_signature
            };
        }
    },
    (error, tracks: typeTrack[]) => {
        if (error) {
            console.error(error);
        }

        console.log("Tracks:", tracks.length);

        // Extract distinct artists from all tracks for artist dataset filtering
        const artistArrays = [];
        
        // a track can have multiple artists - need to create a flattened array
        tracks.forEach(x => artistArrays.push(x.id_artists));
        const trackArtists: string[] = artistArrays.flatMap(
            a => a.map(
                b => String(b).trim()
            )
        );

        // Distinct artists
        const uniqueTrackArtists = new Set(trackArtists);
        console.log("Distinct tracks' artists:", uniqueTrackArtists.size);

        // Load artists dataset
        fileContent = fs.readFileSync(artistsFilePath, { encoding: 'utf-8' });

        parse(
            fileContent,
            {
                delimiter: ',',
                columns: true,  // first line as a header and starts parsing from the 2nd line
                cast: (columnValue, context) => {
                    // data type conversion
                    if (context.column === 'followers' || context.column === 'popularity') {
                        return parseInt(columnValue);
                    }

                    // data type conversion
                    if (context.column === 'genres') {
                        return columnValue.replace('[','').replace(']','').split(' ');
                    }
                
                    return columnValue;
                },
                on_record: (line) => {
                    // Load only these artists that have tracks after the filtering
                    if (uniqueTrackArtists.has(line.id)) {
                        return line;
                    }

                    return;
                }
            },
            async (error, artists: typeArtist[]) => {
                if (error) {
                    console.error(error);
                }

                console.log("Artists:", artists.length);

                // Insert the data into the PostgreSQL database
                //await insertArtists(artists);
                await insertTracks(tracks);
            }
        );
    }
);


function connectPostgreSQL () {
    try {
        const client = new Client({
            user: 'postgres',   // a service user with specific accesses would be used here
            host: 'localhost',  // a dynamic host variable according to environment would be used here
            database: 'postgres',
            password: 'kreditas',   // AWS Secret Manager secret would be used here
            port: 5432,
        });

        return client;
    } catch (err) {
        console.error('Error creating client:', err);
    }
}


function buildIntermediateInsert (table: String, keyVal: String, dataArr: String[]) {
    try {
        if (dataArr.length == 0) {
            return '';
        }

        let query: string = ``;
        let queryCommand: string = ``;
        let index: number = 0;

        for (const iteratorVal of dataArr) {
            if (index == 0) {
                switch (table) {
                    case 'TrackArtists':
                        queryCommand = `
                        INSERT INTO public."${table}" ("TrackId","ArtistId") 
                        VALUES (`;
                        break;
                    case 'ArtistGenres':
                        queryCommand = `
                        INSERT INTO public."${table}" ("ArtistId","GenreName") 
                        VALUES (`;
                        break;
                ////
                ////    Add any number of intermediate table (one-to-many) handlers
                ////
                    default:
                        throw "No table recognised!";
                }
            }
            else {
                queryCommand = `
                ,(`;
            }
            query = query.concat(`${queryCommand}'${keyVal}', '${iteratorVal.trim()}')`);
    
            index++;
        }

        return query;
    } catch (err) {
        console.error('Error building intermediate query:', err);
    }
}


async function insertArtists(artists: typeArtist[]) {
    const client = connectPostgreSQL();
    let mainInsertCount: number = 0;
  
    try {
        await client.connect();

        // Clean previous data
        //client.query(`TRUNCATE public."Artists" RESTART IDENTITY CASCADE`);
        //client.query(`TRUNCATE public."ArtistGenres" RESTART IDENTITY CASCADE`);

        // Insert artists - Batch load would be way faster
        for (const artist of artists) {
            var query = `
                INSERT INTO public."Artists" (
                    "ArtistId", 
                    "ArtistName", 
                    "Followers",
                    "Popularity"
                ) VALUES ($1, $2, $3, $4)`;

            var values = [
                artist.id,
                artist.name,
                Number.isNaN(artist.followers) ? 0 : artist.followers,
                Number.isNaN(artist.popularity) ? 0 : artist.popularity,
            ];

            //await client.query(query, values);
            mainInsertCount++;

            query = buildIntermediateInsert('ArtistGenres', artist.id, artist.genres);

            if (query != '') {
                //await client.query(query);
            }
        }
    } catch (err) {
        console.log('Err artist: ', artists[mainInsertCount]);
        console.error('Error inserting tracks:', err);
    } finally {
        await client.end();
    }
}


async function insertTracks(tracks: typeTrack[]) {
    const client = connectPostgreSQL();
    let mainInsertCount: number = 0;
  
    try {
        await client.connect();

        // Clean previous data
        //client.query(`TRUNCATE public."Tracks" RESTART IDENTITY CASCADE`);
        client.query(`TRUNCATE public."TrackArtists" RESTART IDENTITY CASCADE`);

        // Insert tracks - Batch load would be way faster
        for (const track of tracks) {
            var query = `
                INSERT INTO public."Tracks" (
                    "TrackId", 
                    "TrackName", 
                    "Popularity", 
                    "DurationMS", 
                    "Explicit", 
                    "ReleaseYear", 
                    "ReleaseMonth", 
                    "ReleaseDay", 
                    "Danceability", 
                    "Energy", 
                    "Key", 
                    "Loudness", 
                    "Mode", 
                    "Speechiness", 
                    "Acousticness", 
                    "Instrumentalness", 
                    "Liveness", 
                    "Valence", 
                    "Tempo", 
                    "TimeSignature"
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9
                    ,$10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`;

            var values = [
                track.id,
                track.name,
                track.popularity,
                track.duration_ms,
                track.explicit,
                track.release_year,
                track.release_month,
                track.release_day,
                track.danceability,
                track.energy,
                track.key,
                track.loudness,
                track.mode,
                track.speechiness,
                track.acousticness,
                track.instrumentalness,
                track.liveness,
                track.valence,
                track.tempo,
                track.time_signature
            ];

            //await client.query(query, values);
            mainInsertCount++;

            query = buildIntermediateInsert('TrackArtists', track.id, track.id_artists);

            if (query != '') {
                await client.query(query);
            }
        }
    } catch (err) {
        console.log('Err track: ', tracks[mainInsertCount]);
        console.error('Error inserting tracks:', err);
    } finally {
        await client.end();
    }
}
