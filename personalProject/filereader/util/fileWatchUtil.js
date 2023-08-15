const chokidar = require('chokidar');
const config = require('../config.json')
const writeWaitTime = config.writeWaitTime||2000;
const pollInterval = config.pollInterval||100;
const Queue = require('queue');

const QueueConfig={
    concurrency: config.maxConcurrentJobs||10,
    autostart: true
}
const Q=Queue(QueueConfig);

module.exports = {
    "startFileWatch": startFileWatch
}

function newJOb(path,callback){
    return function(qCallback){
        return callback(path,qCallback);
    } 
}
function startFileWatch(directory, callback){
    return new Promise((resolve, reject)=>{
        chokidar.watch(directory,{
            ignoreInitial: true,
            alwaysStat: true,
            awaitWriteFinish: {
                stabilityThreshold: writeWaitTime,
                pollInterval: pollInterval
            }
        }).on('change', (path, stats) => {
            console.log("change detected in :", path);
            Q.push(newJOb(path,callback));
        }).on('add', (path,stats ) => {
            console.log("added : ", path);
            Q.push(newJOb(path,callback));
        });
    })
}
