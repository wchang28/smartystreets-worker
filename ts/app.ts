import * as AWS from "aws-sdk";
import * as fs from "fs";
import {S3} from "aws-sdk";
import * as url from "url";
import {Readable, Transform} from "stream";
import {get as getHTTPBlob, ReadableContent} from "node-http-blob-stream";
import csv = require('csv');
import {ObjectTransformStream} from "object-transform-stream";
import {aggregate, un_aggregate} from "object-aggregate-stream";
import {USStreetAddress} from "smartystreets-types";
import {NormalizationResult, normalize, normalize_query, concurrent_normalize} from "./smartystreets-us-addr-normalization-stream";

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
function getS3FileStream(Bucket: string, Key: string) : Promise<Readable> {return getS3SignedUrl(Bucket, Key).then((url: string) => getHTTPBlob(url)).then((rc: ReadableContent<Readable>) => Promise.resolve<Readable>(rc.readable));}

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
});

let requestAnalyzingStream = new ObjectTransformStream<USStreetAddress.QueryParamsItem[][], USStreetAddress.QueryParamsItem[][]>((input: USStreetAddress.QueryParamsItem[][]) => {
    let count = 0;
    for (let i in input) {
        count += input[i].length;
    }
    console.log("<REQUEST>: " + count);
    // TODO: write to database
    return Promise.resolve<USStreetAddress.QueryParamsItem[][]>(input);
});

let progressRecorderStream = new ObjectTransformStream<NormalizationResult, USStreetAddress.QueryResultItem[]>((result: NormalizationResult) => {
    let RequestCount = result.RequestCount;
    let NormalizedCount = result.QueryResult.length;
    console.log("<PROGRESS>:" + [RequestCount, NormalizedCount].join(","));
    // TODO: write to database
    return Promise.resolve<USStreetAddress.QueryResultItem[]>(result.QueryResult);
});

let csvOutputHeaders: string[] = [
    "Id"
    ,"delivery_line_1"
    ,"delivery_line_2"
    ,"last_line"
    ,"delivery_point_barcode"
    ,"components.primary_number"
    ,"components.street_name"
    ,"components.street_predirection"
    ,"components.street_postdirection"
    ,"components.street_suffix"
    ,"components.secondary_number"
    ,"components.secondary_designator"
    ,"components.extra_secondary_number"
    ,"components.extra_secondary_designator"
    ,"components.pmb_designator"
    ,"components.pmb_number"
    ,"components.city_name"
    ,"components.default_city_name"
    ,"components.state_abbreviation"
    ,"components.zipcode"
    ,"components.plus4_code"
    ,"components.delivery_point"
    ,"components.delivery_point_check_digit"
    ,"metadata.county_fips"
    ,"metadata.county_name"
    ,"metadata.carrier_route"
    ,"metadata.congressional_district"
    ,"metadata.rdi"
    ,"metadata.latitude"
    ,"metadata.longitude"
    ,"metadata.precision"
    ,"metadata.time_zone"
    ,"metadata.utc_offset"
    ,"metadata.dst"
];

let qri2RowStream = new ObjectTransformStream<USStreetAddress.QueryResultItem, string[]>((item: USStreetAddress.QueryResultItem) => {
    let row: string[] = [
        item.input_id ? item.input_id: ""
        ,item.delivery_line_1 ? item.delivery_line_1 : ""
        ,item.delivery_line_2 ? item.delivery_line_2 : ""
        ,item.last_line ? item.last_line : ""
        ,item.delivery_point_barcode ? item.delivery_point_barcode : ""
        ,item.components && item.components.primary_number ? item.components.primary_number : ""
        ,item.components && item.components.street_name ? item.components.street_name : ""
        ,item.components && item.components.street_predirection ? item.components.street_predirection : ""
        ,item.components && item.components.street_postdirection ? item.components.street_postdirection : ""
        ,item.components && item.components.street_suffix ? item.components.street_suffix : ""
        ,item.components && item.components.secondary_number ? item.components.secondary_number : ""
        ,item.components && item.components.secondary_designator ? item.components.secondary_designator : ""
        ,item.components && item.components.extra_secondary_number ? item.components.extra_secondary_number : ""
        ,item.components && item.components.extra_secondary_designator ? item.components.extra_secondary_designator : ""
        ,item.components && item.components.pmb_designator ? item.components.pmb_designator : ""
        ,item.components && item.components.pmb_number ? item.components.pmb_number : ""
        ,item.components && item.components.city_name ? item.components.city_name : ""
        ,item.components && item.components.default_city_name ? item.components.default_city_name : ""
        ,item.components && item.components.state_abbreviation ? item.components.state_abbreviation : ""
        ,item.components && item.components.zipcode ? item.components.zipcode : ""
        ,item.components && item.components.plus4_code ? item.components.plus4_code : ""
        ,item.components && item.components.delivery_point ? item.components.delivery_point : ""
        ,item.components && item.components.delivery_point_check_digit ? item.components.delivery_point_check_digit : ""
        ,item.metadata && item.metadata.county_fips ? item.metadata.county_fips : ""
        ,item.metadata && item.metadata.county_name ? item.metadata.county_name : ""
        ,item.metadata && item.metadata.carrier_route ? item.metadata.carrier_route : ""
        ,item.metadata && item.metadata.congressional_district ? item.metadata.congressional_district : ""
        ,item.metadata && item.metadata.rdi ? item.metadata.rdi : ""
        ,item.metadata && item.metadata.latitude ? item.metadata.latitude.toString() : ""
        ,item.metadata && item.metadata.longitude ? item.metadata.longitude.toString() : ""
        ,item.metadata && item.metadata.precision ? item.metadata.precision : ""
        ,item.metadata && item.metadata.time_zone ? item.metadata.time_zone : ""
        ,item.metadata && item.metadata.utc_offset ? item.metadata.utc_offset.toString() : ""
        ,item.metadata && item.metadata.dst ? item.metadata.dst.toString() : ""
    ];
    return Promise.resolve<string[]>(row);
});

/*
getFileStream(FilePath)
.then((rs: Readable) => {
    let csvParser: Transform = csv.parse();
    
    //csvParser.on("fisish", () => {
    //    console.log("csvParser: <<FINISH>>");
    //}).on("end", () => {
    //    console.log("csvParser: <<END>>");
    //});
    
    ssreqts.on("data", (qpi: USStreetAddress.QueryParamsItem) => {
        //console.log(JSON.stringify(qpi));
    }).on("finish", () => {
        //console.log("ssreqts: <<FINISH>>");
    }).on("end", () => {
        //console.log("ssreqts: <<END>>");
        console.log("Transformed = " + ssreqts.Transformed);
    });
    
    let aggregateStream = aggregate(100);
    //let count = 0;
    //aggregateStream.on("data", (query: USStreetAddress.QueryParamsItem[]) => {
    //    console.log(JSON.stringify(query, null, 2));
        //console.log(query.length);
    //    count += query.length;
    //}).on("finish", () => {
    //    console.log("aggregateStream: <<FINISH>>");
    //}).on("end", () => {
    //    console.log("aggregateStream: <<END>>");
    //    console.log("count = " + count);
    //});
    
    let normalizationStream = normalize();

    let count = 0;
    normalizationStream.on("data", (result: USStreetAddress.QueryResult) => {
        //console.log(JSON.stringify(result, null, 2));
        console.log(result.length);
        count += result.length;
    }).on("finish", () => {
        console.log("normalizationStream: <<FINISH>>");
    }).on("end", () => {
        console.log("normalizationStream: <<END>>");
        console.log("count=" + count);
    });

    rs.pipe(csvParser).pipe(ssreqts).pipe(aggregateStream).pipe(normalizationStream);
}).catch((err: any) => {
    console.error("!!! Error: " + JSON.stringify(err));
    process.exit(1);
});
*/

getFileStream(FilePath)
.then((rs: Readable) => {
    let csvParser: Transform = csv.parse();
    let aggregateStream1 = aggregate(100);
    let aggregateStream2 = aggregate(20);
    let concurrentNormalizationStream = concurrent_normalize();
    let unAggregateStream = un_aggregate();
    
    //ssreqts.on("data", (qpi: USStreetAddress.QueryParamsItem) => {
        //console.log(JSON.stringify(qpi));
    //}).on("finish", () => {
        //console.log("ssreqts: <<FINISH>>");
    //}).on("end", () => {
        //console.log("ssreqts: <<END>>");
    //    console.log("Transformed = " + ssreqts.Transformed);
    //});
    
    //let count = 0;
    //aggregateStream2.on("data", (queries: USStreetAddress.QueryParamsItem[][]) => {
        //console.log(JSON.stringify(qpis, null, 2));
    //}).on("finish", () => {
    //    console.log("aggregateStream2: <<FINISH>>");
    //}).on("end", () => {
    //    console.log("aggregateStream2: <<END>>");
    //    console.log("count = " + count);
    //});
        
    let count = 0;
    concurrentNormalizationStream.on("data", (result: NormalizationResult) => {
        //console.log(JSON.stringify(result, null, 2));
        //console.log([result.RequestCount,result.QueryResult.length].join(","));
        count += result.QueryResult.length;
    }).on("finish", () => {
        console.log("concurrentNormalizationStream: <<FINISH>>");
    }).on("end", () => {
        console.log("concurrentNormalizationStream: <<END>>");
        console.log("count=" + count);
    });

    /*
    let count = 0;
    unAggregateStream.on("data", (item: USStreetAddress.QueryResultItem) => {
        count++;
    }).on("finish", () => {
        console.log("unAggregateStream: <<FINISH>>");
    }).on("end", () => {
        console.log("unAggregateStream: <<END>>");
        console.log("count=" + count);
    });
    */
    /*
    let count = 0;
    qri2RowStream.on("data", (row: string[]) => {
        console.log(row);
        count++;
    }).on("finish", () => {
        console.log("qri2RowStream: <<FINISH>>");
    }).on("end", () => {
        console.log("qri2RowStream: <<END>>");
        console.log("count=" + count);
    });
    */

    let csvStringifier: Transform = csv.stringify();
    csvStringifier.write(csvOutputHeaders);

    let ws = fs.createWriteStream("c:/tmp/output.csv");

    rs.pipe(csvParser)
    .pipe(ssreqts)
    .pipe(aggregateStream1)
    .pipe(aggregateStream2)
    .pipe(requestAnalyzingStream)
    .pipe(concurrentNormalizationStream)
    .pipe(progressRecorderStream)
    .pipe(unAggregateStream)
    .pipe(qri2RowStream)
    .pipe(csvStringifier)
    .pipe(ws);
}).catch((err: any) => {
    console.error("!!! Error: " + JSON.stringify(err));
    process.exit(1);
});
