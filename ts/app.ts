import * as AWS from "aws-sdk";
import * as fs from "fs";
import {S3} from "aws-sdk";
import {Readable, Transform} from "stream";
import {get as getHTTPBlob, ReadableContent} from "node-http-blob-stream";
import csv = require('csv');
import {ObjectTransformStream} from "object-transform-stream";
import {aggregate} from "object-aggregate-stream"
import {USStreetAddress} from "smartystreets-types";
import {normalize} from "./smartystreets-us-addr-normalization-stream"

let AUTH_ID = process.env["SMARTYSTREETS_AUTH_ID"];
let AUTH_TOKEN = process.env["SMARTYSTREETS_AUTH_TOKEN"];
console.log("AUTH_ID=" + AUTH_ID);
console.log("AUTH_TOKEN=" + AUTH_TOKEN);

/*
AWS.config.credentials = new AWS.SharedIniFileCredentials({profile: "zzyzx"});
let Bucket = "ksfglfgnfg";
let Key = "fslghlfgs.csv";
*/

let FilePath = process.argv[2];
if (!FilePath) {
    console.error("csv file path is not optional");
    process.exit(1);
}

if (!AUTH_ID) {
    console.error("env. var. SMARTYSTREETS_AUTH_ID is not set");
    process.exit(1);  
}

if (!AUTH_TOKEN) {
    console.error("env. var. SMARTYSTREETS_AUTH_TOKEN is not set");
    process.exit(1);  
}

function getS3SignedUrl(Bucket: string, Key: string) : Promise<string> {
    return new Promise<string>((resolve: (value: string) => void, reject: (err: any) => void) => {
        let s3 = new S3();
        s3.getSignedUrl("getObject", {Bucket, Key, Expires: 15}, (err: any, url: string) => {
            if (err)
                reject(err);
            else
                resolve(url);
        })
    });
}

function getFileStream(filePath: string) : Promise<Readable> {return Promise.resolve<Readable>(fs.createReadStream(filePath));}
function getS3FileStream(Bucket, Key) : Promise<Readable> {return getS3SignedUrl(Bucket, Key).then((url: string) => getHTTPBlob(url)).then((rc: ReadableContent<Readable>) => Promise.resolve<Readable>(rc.readable));}

let ColumnIndices: {[column: string]: number} = {
    "id": 0
    ,"address": 1
    ,"zip": 2
};

let ssreqts = new ObjectTransformStream<string[], USStreetAddress.QueryParamsItem>((row: string[]) => {
    let idx_id = ColumnIndices["id"];
    let idx_address = ColumnIndices["address"];
    let idx_zip = ColumnIndices["zip"];
    let qpi: USStreetAddress.QueryParamsItem = {
        input_id: row[idx_id]
        ,street: row[idx_address]
        ,zipcode: row[idx_zip]
        ,candidates: 1
    };
    return Promise.resolve<USStreetAddress.QueryParamsItem>(qpi);
}, (row: string[], index: number) => {
    let include = true;
    if (row.length < 3)
        include = false;
    else {  // record.length >= 3
        if (index == 0) {   // first row => could column headers
            let t = row.map<string>((value: string) => value.toLowerCase());
            let idx_id = t.indexOf("id");
            let idx_address = t.indexOf("address");
            let idx_zip = t.indexOf("zip");
            if (idx_id !== -1 && idx_address !== -1 && idx_zip !== 1) { // first row is column headers
                ColumnIndices["id"] = idx_id;
                ColumnIndices["address"] = idx_address;
                ColumnIndices["zip"] = idx_zip;
                include = false;
            }
        }
    }
    return Promise.resolve<boolean>(include);
})

getFileStream(FilePath)
.then((rs: Readable) => {
    let csvParser: Transform = csv.parse();
    /*
    csvParser.on("fisish", () => {
        console.log("csvParser: <<FINISH>>");
    }).on("end", () => {
        console.log("csvParser: <<END>>");
    });
    */

    ssreqts.on("data", (qpi: USStreetAddress.QueryParamsItem) => {
        //console.log(JSON.stringify(qpi));
    }).on("finish", () => {
        //console.log("ssreqts: <<FINISH>>");
    }).on("end", () => {
        //console.log("ssreqts: <<END>>");
        console.log("Transformed = " + ssreqts.Transformed);
    });
    
    let aggregateStream = aggregate(100);
    /*
    let count = 0;
    aggregateStream.on("data", (qpis: USStreetAddress.QueryParamsItem[]) => {
        console.log(JSON.stringify(qpis, null, 2));
        //console.log(qpis.length);
        count += qpis.length;
    }).on("finish", () => {
        console.log("aggregateStream: <<FINISH>>");
    }).on("end", () => {
        console.log("aggregateStream: <<END>>");
        console.log("count = " + count);
    });
    */


    let normalizationStream = normalize();

    normalizationStream.on("data", (result: USStreetAddress.QueryResult) => {
        //console.log(JSON.stringify(result, null, 2));
        console.log(result.length);
    }).on("finish", () => {
        console.log("normalizationStream: <<FINISH>>");
    }).on("end", () => {
        console.log("normalizationStream: <<END>>");
    });

    rs.pipe(csvParser).pipe(ssreqts).pipe(aggregateStream).pipe(normalizationStream);
}).catch((err: any) => {
    console.log("!!! Error: " + JSON.stringify(err));
    process.exit(0);
})
