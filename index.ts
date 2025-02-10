import * as fs from 'fs';
//import * as rd from 'readline';
//import * as http from 'http';
import { parse } from 'csv-parse';
import { Client } from 'pg';

let tracksFilePath = 'data/tracks_small.csv';
let artistsFilePath = 'data/artists_small.csv';

//id,followers,genres,name,popularity
type typeArtist = {    
    id: String,
    followers: number,
    genres: String[],
    name: String,
    popularity: number
}

//var artists: Array<artist> = [];

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

var tracks: Array<typeTrack>;

/*
let tracks_artists;
function explodeArray(value, index, array) {
    //console.log(value.id_artists);
    tracks_artists.push(
        [].concat.apply([], value.map(function (track) { return track.id_artists; }))
    );
}
const numbers = [45, 4, [9, 9], 16, 25];
const other = [3, 3, 55];

let txt = "";
numbers.forEach(myFunction);
document.getElementById("demo").innerHTML = txt;

function myFunction(value) {
  other.push(value.flatMap(a => a)); 
}
*/

//console.log(typeof tracks['popularity']);
//console.log(typeof tracks['artists']);
//console.log(typeof tracks['energy']);
//console.log(typeof tracks['id']);
//type ColumnType = typeof tracks['popularity'];

//var headers = ['id','name','popularity','duration_ms','explicit','artists','id_artists','release_date','danceability','energy','key','loudness','mode','speechiness','acousticness','instrumentalness','liveness','valence','tempo','time_signature'];
var fileContent = fs.readFileSync(tracksFilePath, { encoding: 'utf-8' });

parse(
    fileContent,
    {
        delimiter: ',',
        columns: true,  // first line as a header and starts parsing from the 2nd line
        cast: (columnValue, context) => {
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

            if (context.column === 'artists'
                || context.column === 'id_artists'
            ) {
                return columnValue.replace(/\]|\[|\'/g,'').split(',');
            }

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

        //console.log("Tracks:", tracks);
        console.log("Tracks:", tracks.length);

        // Extract distinct artists from all tracks for filtering
        // a track can have multiple artists - need to create a flattened array
        const artistArrays = [];
        tracks.forEach(x => artistArrays.push(x.id_artists));
        //console.log("artistArrays:", artistArrays);

        const trackArtists: string[] = artistArrays.flatMap(
            a => a.map(
                b => String(b).trim()
            )
        );

        // all track artists
        //console.log("Track artists:", track_artists);

        // Distinct artists
        const uniqueTrackArtists = new Set(trackArtists);
        console.log("Compare u tracks:", uniqueTrackArtists.size);

        fileContent = fs.readFileSync(artistsFilePath, { encoding: 'utf-8' });

        parse(
            fileContent,
            {
            delimiter: ',',
            columns: true,
            cast: (columnValue, context) => {
                if (context.column === 'followers' || context.column === 'popularity') {
                return parseInt(columnValue);
                }

                if (context.column === 'genres') {
                return columnValue.replace('[','').replace(']','').split(' ');
                }
            
                return columnValue;
            },
            on_record: (line, context) => {
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
                //console.log("Artists:", artists);

                await insertArtists(artists);
                await insertTracks(tracks);
            }
        );
    }
);


function connectPostgreSQL () {
    const client = new Client({
        user: 'postgres',
        host: 'localhost',
        database: 'postgres',
        password: 'kreditas',
        port: 5432,
    });

    return client;
}


function buildIntermediateInsert (table: String, keyVal: String, dataArr: String[]) {
    try {
        if (dataArr.length == 0 || dataArr[0] != '') {
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
            query = query.concat(`${queryCommand}'${keyVal}', '${iteratorVal}')`);
    
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
    let interInsertCount: number = 0;
  
    try {
        await client.connect();

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
                artist.followers,
                artist.popularity,
            ];
            await client.query(query, values);
            mainInsertCount++;

            query = buildIntermediateInsert('ArtistGenres', artist.id, artist.genres);

            //console.log(query);
            if(query != '') {
                client.query(query);
                interInsertCount++;
            }
        }
  
        console.log(`${mainInsertCount} Artists inserted successfully`);
        console.log(`${interInsertCount} Genres inserted successfully`);
    } catch (err) {
        console.error('Error inserting tracks:', err);
    } finally {
        await client.end();
    }
}


async function insertTracks(tracks: typeTrack[]) {
    const client = connectPostgreSQL();
    let mainInsertCount: number = 0;
    let interInsertCount: number = 0;
  
    try {
        await client.connect();

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
            await client.query(query, values);
            mainInsertCount++;

            query = buildIntermediateInsert('TrackArtists', track.id, track.id_artists);

            //console.log(query);
            if(query != '') {
                client.query(query);
                interInsertCount++;
            }
        }
  
        console.log(`${mainInsertCount} Tracks inserted successfully`);
        console.log(`${interInsertCount} TrackArtists inserted successfully`);
    } catch (err) {
        console.error('Error inserting tracks:', err);
    } finally {
        await client.end();
    }
}


//console.log(`art: ${artists.length}`);

/*
function readLineArtist(filePath: fs.PathLike): artist {
    let reader = rd.createInterface(fs.createReadStream(filePath));
    var parsed: artist;

    reader.on("line", (line: string) => {
        let attr = line.split(',');
        
        //let id = attr[0];
        //let followers = parseInt(attr[1]);
        //let genres = attr[2].split(' ');
        //let name = attr[3];
        //let popularity = parseInt(attr[4]);
    
        parsed = {
            id: attr[0],
            followers: parseInt(attr[1]),
            genres: attr[2].split(' '),
            name: attr[3],
            popularity: parseInt(attr[4])
        };

        //console.log(`followers: ${followers} genres: ${genres} name: ${name} popularity: ${popularity}`);
        //artists.push({
        //    id, followers, genres, name, popularity
        //});
        console.log(`arta: ${parsed}`)
        artists.push(parsed);
    });

    console.log(`artb: ${parsed}`)

    return parsed;
}

readLineArtist('data/artists_small.csv');
console.log(`artc: ${artists.length}`);
*/




/*
console.log(`art: ${artists.length}`);
console.log(`abc: ${artists[5]}`);
//console.log(`def: ${artist[5].name}`);

reader = rd.createInterface(fs.createReadStream('data/tracks_small.csv'));

reader.on("line", (l: string) => {
    let attr = l.split(',');
    
    let id = attr[0];
    let name = attr[1];
    let popularity = parseInt(attr[2]);
    let duration_ms = parseInt(attr[3]);
    let explicit = parseInt(attr[4]);
    let artists = attr[5].split(',');
    let id_artists = attr[6].split(',');
    let release_date = attr[7].split('-');
    let danceability = parseFloat(attr[8]);
    let energy = parseFloat(attr[9]);
    let key = parseFloat(attr[10]);
    let loudness = parseFloat(attr[11]);
    let mode = parseFloat(attr[12]);
    let speechiness = parseFloat(attr[13]);
    let acousticness = parseFloat(attr[14]);
    let instrumentalness = parseFloat(attr[15]);    //e
    let liveness = parseFloat(attr[16]);
    let valence = parseFloat(attr[17]);
    let tempo = parseFloat(attr[18]);
    let time_signature = parseInt(attr[19]);

    let releaseYYYY = parseInt(release_date[0]);
    let releaseMM = parseInt(release_date[1]);
    let releaseDD = parseInt(release_date[2]);

    console.log(`name: ${name} Y: ${releaseYYYY} M: ${releaseMM} numbah: ${instrumentalness}`);
    tracks.push({
        id,name,popularity,duration_ms,explicit,artists,id_artists,release_date,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,time_signature
    });
});

console.log(`tra: ${tracks.length}`);
console.log(`ack: ${tracks[5]}`);

*/

/*****************************************************************/

//var content[] = await fs.promises.readFile('data/tracks_small.csv');
// Parse the CSV content
//const records = parse(content, {delimiter: ","});
//console.log(`records: ${records}`);

/*
var records = [];

// Initialize the parser
const parser = parse({
    delimiter: ","
});

// Use the readable stream api to consume records
parser.on("readable", function () {
    let record;
    while ((record = parser.read()) !== null) {
        records.push(record);
    }
});
*/






/*
var data: Array<{ number: number; from: string; to: string}> = [];
reader.on("line", (l: string) => {
    var tokens = l.split(' ');
    var nr= parseInt(tokens[0]);
    var from = tokens[1];
    var to = tokens[2]
    console.log(`nr: ${nr} from ${from} to ${to}`);
    data.push({
        number: nr, from, to
    });
})
console.log(`Will be empty data has not yet been read ${data.length}` );

reader.on("close", ()=> {
    console.log(`Data has been read ${data.length}` );
    data.forEach(element => {
        console.log(`nr: ${element.number} from ${element.from} to ${element.to}`)
    });
})







var http = require('http');
var fs = require('fs');

http.createServer(function (req, res) {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.write('Hello World!');

    fs.readFile('data/artists_small.csv', function(err, data) {
        //res.writeHead(200, {'Content-Type': 'text/html'});
        res.write(data);
        return res.end();
    });

}).listen(8080);
*/
