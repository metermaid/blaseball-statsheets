/*
this is basically @shibboh's code that I bastardized for my own purposes
credit for anything good goes to him and library authors
credit for the rest to me
*/

const Bottleneck = require("bottleneck/es5");
const fetch = require("node-fetch-retry");
const fs = require('fs');
const merge = require('deepmerge');
const ndjson = require('ndjson');

const limiter = new Bottleneck({
    maxConcurrent: 1,
    minTime: 150,
});

function getCurrentIds(fileName, defaultValue) {
    try {
        let rawdata = fs.readFileSync(fileName);
        return JSON.parse(rawdata);
    } catch (err) {
        return defaultValue;
    }
    return defaultValue;
}

function chunkArray(arr) {
    return [...arr].reduce((resultArray, item, index) => { 
        const chunkIndex = Math.floor(index/100);

        if(!resultArray[chunkIndex]) {
            resultArray[chunkIndex] = [] // keeping it chonky
        }

        resultArray[chunkIndex].push(item);

        return resultArray;
    }, []);
}

async function getCurrentStatsheetIds(fileName) {
    let input = fs.createReadStream(fileName);
    let ids = [];
    await new Promise((resolve, reject) => {
        input.on('error', function (error) {resolve();})
        .pipe(ndjson.parse()).on('data', function(obj) {
            ids.push(obj.id);
        }).on("error", (err) => {
            reject(err);
        }).on("finish", function() {
            resolve();
        });
    });
    return ids;
}

async function saveIds(fileName, ids) {
    console.log("Saving " + fileName);
    await fs.writeFile(fileName, `${JSON.stringify(ids, null, "\t")}\n`, function (err) {
        if (err) {
            console.log(err);
        }
    });
}

async function fetchStatsheet(prefix, ids, endpoint, callback) {
    const idfile = `./data/ids/${prefix}StatsheetIds.json`;
    const sheetfile = `./data/statsheets/${prefix}Statsheets.json`;

    await saveIds(idfile, ids);

    const processedIds = await getCurrentStatsheetIds(sheetfile);
    const processedIdsSet = new Set(processedIds);
    console.log(`all ${prefix} statsheets: ${Object.keys(ids).length}`);
    console.log(`processed ${prefix} statsheets: ${processedIds.length}`);
    var justIds = Object.keys(ids).filter(x => !processedIdsSet.has(x));
    var chunkedids = chunkArray(justIds);
    console.log(`remaining ${prefix} statsheets: ${justIds.length}`);

    // Create write stream for game statsheet responses
    const writeStream = fs.createWriteStream(sheetfile, {flags:'a'});
    // Fetch game statsheets
    console.log("Fetching from " + endpoint);
    await Promise.all(chunkedids.map(async (idGroup) => {
        const response = await limiter.schedule(getData, `${endpoint}${idGroup.join(',')}`);
        for (const sheet of response) {
            const contextObj = ids[sheet.id];
            contextObj[`${prefix}StatsheetId`] = sheet.id;
            const newSheet = Object.assign({}, sheet, contextObj);
            callback(newSheet, contextObj);
            writeStream.write(`${JSON.stringify(newSheet)}\n`);
        }
    }));
}

async function fetchStatsheets() {

    await fs.promises.mkdir("./data", { recursive: true });
    await fs.promises.mkdir("./data/statsheets", { recursive: true });
    await fs.promises.mkdir("./data/ids", { recursive: true });

    var gameStatsheetIds = getCurrentIds(`./data/ids/gameStatsheetIds.json`, {});
    var teamStatsheetIds = getCurrentIds(`./data/ids/teamStatsheetIds.json`, {});
    var playerStatsheetIds = getCurrentIds(`./data/ids/playerStatsheetIds.json`, {});
    var playerIds = getCurrentIds(`./data/ids/playerIds.json`, []);

    const games = await getAllGameResults();
    await saveIds("./data/gameResults.json", games);

    // Fetch game statsheets
    for (const season in games) {
        for (const day in games[season]) {
            for (const game of games[season][day]) {
                gameStatsheetIds[game.statsheet] = {"gameId": game.id, "season": season, "day": day};
            }
        }
    }

    await fetchStatsheet(
        "game", 
        gameStatsheetIds, 
        "https://www.blaseball.com/database/gamestatsheets?ids=", 
        (sheet, contextObj) => {
            teamStatsheetIds[sheet.awayTeamStats] = contextObj;
            teamStatsheetIds[sheet.homeTeamStats] = contextObj;
        });

    await fetchStatsheet(
        "team", 
        teamStatsheetIds, 
        "https://www.blaseball.com/database/teamstatsheets?ids=", 
        (sheet, contextObj) => {
            for (const playerStatsheetId of sheet.playerStats) {
                playerStatsheetIds[playerStatsheetId] = contextObj;
            }
        });

    await fetchStatsheet(
        "player", 
        playerStatsheetIds, 
        "https://www.blaseball.com/database/playerSeasonStats?ids=", 
        (sheet, contextObj) => {
            playerIds.push(sheet.playerId);
        });

   await saveIds("./data/ids/playerIds.json", playerIds);
}

async function getData(link) {
    const request = await fetch(link, {
        retryOptions: {
            retryOnHttpResponse: function (response) {
                if (response.status >= 500 || response.status >= 400) {
                    return true;
                }
            },
        },
    });
    const response = await request.json();
    return response;
}

async function fetchGameResults({ startingDay, startingSeason, }) {
    let season = startingSeason;
    let day = startingDay;
    const gameResults = {};
    const url = new URL("https://www.blaseball.com/database/games");
    url.searchParams.set("season", season.toString());
    url.searchParams.set("day", day.toString());
    let games = await limiter.schedule(getData, url);
    let hasActiveGame = false;
    // Create fetch loop to iterate through all days in a season until reaching an empty array
    while (!hasActiveGame && Array.isArray(games) && games.length !== 0) {
        console.log(`Fetched game results for season ${season} day ${day}`);
        // Break out of fetch loop if in-progress games are found
        // - Exclude season 3 due to some games incorrectly marked as not complete
        for (const game of games) {
            if (Number(game.season) !== 3 && game.gameComplete === false) {
                hasActiveGame = true;
                break;
            }
        }
        if (hasActiveGame) {
            break;
        }
        // Store game results of day
        if (!Object.hasOwnProperty.call(gameResults, season)) {
            gameResults[season] = {};
        }
        if (!Object.hasOwnProperty.call(gameResults[season], day)) {
            gameResults[season][day] = games;
        }
        day += 1;
        // Begin new fetch loop
        const url = new URL("https://www.blaseball.com/database/games");
        url.searchParams.set("season", season.toString());
        url.searchParams.set("day", day.toString());
        games = await limiter.schedule(getData, url);
        // When at the end of a season, try to jump to next season
        if (Array.isArray(games) && games.length === 0) {
            season += 1;
            day = 0;
            // Begin new fetch loop
            const url = new URL("https://www.blaseball.com/database/games");
            url.searchParams.set("season", season.toString());
            url.searchParams.set("day", day.toString());
            games = await limiter.schedule(getData, url);
        }
    }
    return gameResults;
}

async function getAllGameResults() {
    let games = {};
    let startingSeason = 0;
    let startingDay = 0;

    try {
        games = await JSON.parse(fs.readFileSync("./data/gameResults.json", "utf8"));
        startingSeason = Object.keys(games)
            .map((season) => Number(season))
            .sort((a, b) => a - b)
            .pop() || startingSeason;
        startingDay = games[startingSeason] && Object.keys(games[startingSeason])
            .map((day) => Number(day))
            .sort((a, b) => a - b)
            .pop() || startingDay;
    } catch (err) {
        console.log(err);
    }
    const newGames = await fetchGameResults({
        startingDay: startingDay + 1,
        startingSeason,
    });
    games = merge(games, newGames);
    return games;
}

fetchStatsheets();