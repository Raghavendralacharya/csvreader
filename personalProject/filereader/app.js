
// const dbConfig = require("./app/config/db.config");
const config  = require('./config.json')

let dbconn = require('./myscript');
dbconn.then((res)=>{
    global.db = res;
})

const fileWatchUtil = require('./util/fileWatchUtil');
const fileProcessingUtil = require('./util/fileProcessingUtil');

function fileWatchStart(app){
    fileWatchUtil.startFileWatch(config.folderConfig.path, fileProcessingUtil.process)
    .then(()=>{
        console.log("File watch started ");
    }).catch((err)=>{
        console.error("File watch stopped due to :");
        console.error(err);
        process.exit(1);
    })    
}


fileWatchStart()
