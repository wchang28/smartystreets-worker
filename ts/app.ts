import * as AWS from "aws-sdk";
import {S3} from "aws-sdk";
import {Readable, Transform} from "stream";
import {get as getBlob, ReadableContent} from "node-http-blob-stream";
import csv = require('csv');

AWS.config.credentials = new AWS.SharedIniFileCredentials({profile: "zzyzx"});
let Bucket = "ksfglfgnfg";
let Key = "fslghlfgs.csv";

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

let i = 0;

getS3SignedUrl(Bucket, Key)
.then((url: string) => {
    return getBlob(url)
}).then((rc: ReadableContent<Readable>) => {
    //console.log(JSON.stringify(rc.info, null, 2));
    let rs = rc.readable;
    let csvParser: Transform = csv.parse();
    rs.pipe(csvParser);
    csvParser.on("data", (record: any) => {
        if (i === 0) {
            console.log(JSON.stringify(record));
        }
        i++;
        //console.log(JSON.stringify(record));
    }).on("end", () => {
        console.log("");
        console.log("Done. i = " + i);
    });
}).catch((err: any) => {
    console.log("!!! Error: " + JSON.stringify(err));
    process.exit(0);
})
