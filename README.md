# hw-SpotifyDataIngestion
Homework task "Spotify Data Transformation and Analysis"

Instructions:
1. Prepare Node.js environment with specified npm packages
2. Prepare PostgreSQL database locally: 
    -database script is available in "create-database.sql"
    -credentials are available in source code "index.ts" file function: connectPostgreSQL()
3. Download and place extracted datafiles in project catalog inside /data/
4. Run "npx tsc index.ts" in terminal.
5. Run "node index.js"

This should run all necessary logic to read the files, transform and filter the data.
Then tables and views will be created, data is then loaded there.
