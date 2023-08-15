
const fs = require('fs');
const csvToJson = require('csvtojson');
const config  = require('../config.json')
const dbhandler = require('../dbhandler')

function updateProcessData(json,processData,fileStatusRecordAndTxn){
    processData.recordsRead.push(json.data);
    processData.count++;
    // if(processData.recordsRead.length == config.bulkSize){
    //     processData.promises.push(storeBatch(processData.recordsRead, fileStatusRecordAndTxn));
    //     processData.recordsRead = [];
    // }
}

function readFileBasedOnDelimitter(filePath,fileStatusRecordAndTxn){
    return new Promise((resolve,reject)=>{
        let headersRead = false;
        let processData={recordsRead : [],promises : [],checksumStr:"",count: 0,headerData:{}};
        csvToJson()
        .fromFile(filePath)
        .then((jsonObj)=>{
            console.log(jsonObj);
            let input = {
                location_id: '1235',
                location_name: 'balaji',
                street_number: '234',
                street_name: 'subramanyapura',
                address2:"balaji",
                city: 'bangalore',
                state: 'karnataka',
                zipcode: '560001',
                latitude: '1234',
                longitude: '1234',
                created_by: 'raghav', 
                modified_by: 'raghav'
            }
            dbhandler.insertlocation(jsonObj[0]);
            // if(headersRead){
            //     processData.recordsRead.push(jsonArr)
            // } else {
            //     processData.headerData = jsonArr;
            //     constructHeader(processData)
            //     headersRead = true;
            // }
            // let json = {}
            // let json = getJSONBasedOnDelimittedConfigAndChecksum(config, jsonArr, selectedConfig.additionalValues||{},config,processData.checksumStr);
            // updateProcessData(json,processData,fileStatusRecordAndTxn,config);
            // console.log(JSON.stringify(jsonArr));
        })
    });
}


function inserttotable(){
//   let query =   "insert into location(location_id,location_name, street_number, street_name, address2, city, state, zipcode, latitude, longitude, created_at, created_by, modified_at, modified_by) 
//     values('1234','shrini','abc','achamane','ullur', 'kunda', 'karna','576219', '12345','12345',TO_DATE('2023/05/03', 'yyyy/mm/dd'),'raghav',TO_DATE('2023/05/03', 'yyyy/mm/dd'), 'raghav');"
}

function constructHeader(processData){
    let headerData = []
    for(let i =0;i< processData.headerData.length;i++){
        if(config.columnMap[processData.headerData[i]]){
            headerData.push(config.columnMap[processData.headerData[i]]);
        } else {
            headerData.push(processData.headerData[i]);
        }
    }
    processData.headerData = headerData;
}

function constructTableRecord(){
    
}

module.exports ={
    process:readFileBasedOnDelimitter
}