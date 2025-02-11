"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
var fs = require("fs");
var csv_parse_1 = require("csv-parse");
var pg_1 = require("pg");
var createTables = fs.readFileSync('./create-tables.sql', { encoding: 'utf-8' });
var createViews = fs.readFileSync('./create-views.sql', { encoding: 'utf-8' });
var tracksFilePath = 'data/tracks.csv';
var artistsFilePath = 'data/artists.csv';
try {
    var fileContent = fs.readFileSync(tracksFilePath, { encoding: 'utf-8' });
    // Load tracks dataset
    (0, csv_parse_1.parse)(fileContent, {
        delimiter: ',',
        columns: true, // first line as a header and starts parsing from the 2nd line
        cast: function (columnValue, context) {
            // data type conversion
            if (context.column === 'duration_ms'
                || context.column === 'popularity'
                || context.column === 'explicit'
                || context.column === 'key'
                || context.column === 'mode'
                || context.column === 'time_signature') {
                return parseInt(columnValue);
            }
            // Transform track danceability into string values
            if (context.column === 'danceability') {
                var num = parseFloat(columnValue);
                if (num > 0.6) {
                    return 'High';
                }
                else if (num >= 0.5) {
                    return 'Medium';
                }
                return 'Low';
            }
            // data type conversion
            if (context.column === 'energy'
                || context.column === 'loudness'
                || context.column === 'speechiness'
                || context.column === 'acousticness'
                || context.column === 'instrumentalness'
                || context.column === 'liveness'
                || context.column === 'valence'
                || context.column === 'tempo') {
                return parseFloat(columnValue);
            }
            // data cleanup
            if (context.column === 'artists'
                || context.column === 'id_artists') {
                return columnValue.replace(/\]|\[|\'/g, '').split(',');
            }
            // Explode track release date into separate columns: year, month, day
            if (context.column === 'release_date') {
                return columnValue.split('-');
            }
            return columnValue;
        },
        on_record: function (line, context) {
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
    }, function (error, tracks) {
        if (error) {
            console.error(error);
        }
        console.log("Tracks:", tracks.length);
        // Extract distinct artists from all tracks for artist dataset filtering
        var artistArrays = [];
        // a track can have multiple artists - need to create a flattened array
        tracks.forEach(function (x) { return artistArrays.push(x.id_artists); });
        var trackArtists = artistArrays.flatMap(function (a) { return a.map(function (b) { return String(b).trim(); }); });
        // Distinct artists
        var uniqueTrackArtists = new Set(trackArtists);
        console.log("Distinct tracks' artists:", uniqueTrackArtists.size);
        // Load artists dataset
        fileContent = fs.readFileSync(artistsFilePath, { encoding: 'utf-8' });
        (0, csv_parse_1.parse)(fileContent, {
            delimiter: ',',
            columns: true, // first line as a header and starts parsing from the 2nd line
            cast: function (columnValue, context) {
                // data type conversion
                if (context.column === 'followers' || context.column === 'popularity') {
                    return parseInt(columnValue);
                }
                // data type conversion
                if (context.column === 'genres') {
                    return columnValue.replace('[', '').replace(']', '').split(' ');
                }
                return columnValue;
            },
            on_record: function (line) {
                // Load only these artists that have tracks after the filtering
                if (uniqueTrackArtists.has(line.id)) {
                    return line;
                }
                return;
            }
        }, function (error, artists) { return __awaiter(void 0, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (error) {
                            console.error(error);
                        }
                        console.log("Artists:", artists.length);
                        // Insert the data into the PostgreSQL database
                        return [4 /*yield*/, prepareDatabase()];
                    case 1:
                        // Insert the data into the PostgreSQL database
                        _a.sent();
                        return [4 /*yield*/, insertArtists(artists)];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, insertTracks(tracks)];
                    case 3:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        }); });
    });
}
catch (err) {
    console.error('Error extracting and filtering data:', err);
}
function connectPostgreSQL() {
    try {
        var client = new pg_1.Client({
            user: 'postgres', // a service user with specific accesses would be used here
            host: 'localhost', // a dynamic host variable according to environment would be used here
            database: 'postgres',
            password: 'kreditas', // AWS Secret Manager secret would be used here
            port: 5432,
        });
        return client;
    }
    catch (err) {
        console.error('Error creating client:', err);
    }
}
function prepareDatabase() {
    return __awaiter(this, void 0, void 0, function () {
        var client, err_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    client = connectPostgreSQL();
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 5, 6, 8]);
                    return [4 /*yield*/, client.connect()];
                case 2:
                    _a.sent();
                    // Create tables
                    return [4 /*yield*/, client.query(createTables)];
                case 3:
                    // Create tables
                    _a.sent();
                    // Create views
                    return [4 /*yield*/, client.query(createViews)];
                case 4:
                    // Create views
                    _a.sent();
                    return [3 /*break*/, 8];
                case 5:
                    err_1 = _a.sent();
                    console.error('Error preparing database:', err_1);
                    return [3 /*break*/, 8];
                case 6: return [4 /*yield*/, client.end()];
                case 7:
                    _a.sent();
                    return [7 /*endfinally*/];
                case 8: return [2 /*return*/];
            }
        });
    });
}
function buildIntermediateInsert(table, keyVal, dataArr) {
    try {
        if (dataArr.length == 0) {
            return '';
        }
        var query = "";
        var queryCommand = "";
        var index = 0;
        for (var _i = 0, dataArr_1 = dataArr; _i < dataArr_1.length; _i++) {
            var iteratorVal = dataArr_1[_i];
            if (index == 0) {
                switch (table) {
                    case 'TrackArtists':
                        queryCommand = "\n                        INSERT INTO public.\"".concat(table, "\" (\"TrackId\",\"ArtistId\") \n                        VALUES (");
                        break;
                    case 'ArtistGenres':
                        queryCommand = "\n                        INSERT INTO public.\"".concat(table, "\" (\"ArtistId\",\"GenreName\") \n                        VALUES (");
                        break;
                    ////
                    ////    Add any number of intermediate table (one-to-many) handlers
                    ////
                    default:
                        throw "No table recognised!";
                }
            }
            else {
                queryCommand = "\n                ,(";
            }
            query = query.concat("".concat(queryCommand, "'").concat(keyVal, "', '").concat(iteratorVal.trim(), "')"));
            index++;
        }
        return query;
    }
    catch (err) {
        console.error('Error building intermediate query:', err);
    }
}
function insertArtists(artists) {
    return __awaiter(this, void 0, void 0, function () {
        var client, mainInsertCount, _i, artists_1, artist, query, values, err_2;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    client = connectPostgreSQL();
                    mainInsertCount = 0;
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 8, 9, 11]);
                    return [4 /*yield*/, client.connect()];
                case 2:
                    _a.sent();
                    // Clean previous data
                    client.query("TRUNCATE public.\"Artists\" RESTART IDENTITY CASCADE");
                    client.query("TRUNCATE public.\"ArtistGenres\" RESTART IDENTITY CASCADE");
                    _i = 0, artists_1 = artists;
                    _a.label = 3;
                case 3:
                    if (!(_i < artists_1.length)) return [3 /*break*/, 7];
                    artist = artists_1[_i];
                    query = "\n                INSERT INTO public.\"Artists\" (\n                    \"ArtistId\", \n                    \"ArtistName\", \n                    \"Followers\",\n                    \"Popularity\"\n                ) VALUES ($1, $2, $3, $4)";
                    values = [
                        artist.id,
                        artist.name,
                        Number.isNaN(artist.followers) ? 0 : artist.followers,
                        Number.isNaN(artist.popularity) ? 0 : artist.popularity,
                    ];
                    return [4 /*yield*/, client.query(query, values)];
                case 4:
                    _a.sent();
                    mainInsertCount++;
                    query = buildIntermediateInsert('ArtistGenres', artist.id, artist.genres);
                    if (!(query != '')) return [3 /*break*/, 6];
                    return [4 /*yield*/, client.query(query)];
                case 5:
                    _a.sent();
                    _a.label = 6;
                case 6:
                    _i++;
                    return [3 /*break*/, 3];
                case 7: return [3 /*break*/, 11];
                case 8:
                    err_2 = _a.sent();
                    console.log('Err artist: ', artists[mainInsertCount]);
                    console.error('Error inserting tracks:', err_2);
                    return [3 /*break*/, 11];
                case 9: return [4 /*yield*/, client.end()];
                case 10:
                    _a.sent();
                    return [7 /*endfinally*/];
                case 11: return [2 /*return*/];
            }
        });
    });
}
function insertTracks(tracks) {
    return __awaiter(this, void 0, void 0, function () {
        var client, mainInsertCount, _i, tracks_1, track, query, values, err_3;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    client = connectPostgreSQL();
                    mainInsertCount = 0;
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 8, 9, 11]);
                    return [4 /*yield*/, client.connect()];
                case 2:
                    _a.sent();
                    // Clean previous data
                    client.query("TRUNCATE public.\"Tracks\" RESTART IDENTITY CASCADE");
                    client.query("TRUNCATE public.\"TrackArtists\" RESTART IDENTITY CASCADE");
                    _i = 0, tracks_1 = tracks;
                    _a.label = 3;
                case 3:
                    if (!(_i < tracks_1.length)) return [3 /*break*/, 7];
                    track = tracks_1[_i];
                    query = "\n                INSERT INTO public.\"Tracks\" (\n                    \"TrackId\", \n                    \"TrackName\", \n                    \"Popularity\", \n                    \"DurationMS\", \n                    \"Explicit\", \n                    \"ReleaseYear\", \n                    \"ReleaseMonth\", \n                    \"ReleaseDay\", \n                    \"Danceability\", \n                    \"Energy\", \n                    \"Key\", \n                    \"Loudness\", \n                    \"Mode\", \n                    \"Speechiness\", \n                    \"Acousticness\", \n                    \"Instrumentalness\", \n                    \"Liveness\", \n                    \"Valence\", \n                    \"Tempo\", \n                    \"TimeSignature\"\n                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9\n                    ,$10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)";
                    values = [
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
                    return [4 /*yield*/, client.query(query, values)];
                case 4:
                    _a.sent();
                    mainInsertCount++;
                    query = buildIntermediateInsert('TrackArtists', track.id, track.id_artists);
                    if (!(query != '')) return [3 /*break*/, 6];
                    return [4 /*yield*/, client.query(query)];
                case 5:
                    _a.sent();
                    _a.label = 6;
                case 6:
                    _i++;
                    return [3 /*break*/, 3];
                case 7: return [3 /*break*/, 11];
                case 8:
                    err_3 = _a.sent();
                    console.log('Err track: ', tracks[mainInsertCount]);
                    console.error('Error inserting tracks:', err_3);
                    return [3 /*break*/, 11];
                case 9: return [4 /*yield*/, client.end()];
                case 10:
                    _a.sent();
                    return [7 /*endfinally*/];
                case 11: return [2 /*return*/];
            }
        });
    });
}
