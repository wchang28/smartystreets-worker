import {ObjectTransformStream} from "object-transform-stream";
import {Transform} from "stream";
import {USStreetAddress} from "smartystreets-types";
import * as request from "request";
import JSONStream = require('JSONStream');

export function normalize_query(query: USStreetAddress.QueryParamsItem[]) : Promise<USStreetAddress.QueryResult> {
    return new Promise<USStreetAddress.QueryResult>((resolve: (value: USStreetAddress.QueryResult) => void, reject: (err: any) => void) => {
        let AUTH_ID = process.env["SMARTYSTREETS_AUTH_ID"];
        let AUTH_TOKEN = process.env["SMARTYSTREETS_AUTH_TOKEN"];
        let url = "https://us-street.api.smartystreets.com/street-address?auth-id=" + AUTH_ID + "&auth-token=" + AUTH_TOKEN;
        let result: USStreetAddress.QueryResult = [];
        let jsonParse: Transform = JSONStream.parse('*');
        let req = request.post(url, {json: query});
        req.on("error", (err: any) => {
            reject(err);
        });
        jsonParse.on("data", (record: USStreetAddress.QueryResultItem) => {
            result.push(record);
        }).on("end", () => {
            resolve(result);
        });
        request.post(url, {json: query}).pipe(jsonParse);
    });
}

export function normalize(): Transform {
    return new ObjectTransformStream<USStreetAddress.QueryParamsItem[], USStreetAddress.QueryResult>(normalize_query);
}

export function multi_normalize_query(queries: USStreetAddress.QueryParamsItem[][]) : Promise<USStreetAddress.QueryResult> {
    let promises:Promise<USStreetAddress.QueryResult>[] = []; 
    for (let i in queries) {
        let query = queries[i];
        promises.push(normalize_query(query));
    }
    let p = Promise.all(promises);  
    return p.then((value: USStreetAddress.QueryResult[]) => {
        let ret : USStreetAddress.QueryResult = [];
        for (let i in value) {
            for (let j in value[i]) {
                let item = value[i][j];
                ret.push(item);
            }
        }
        return ret;
    });
}

export function multi_normalize(): Transform {
    return new ObjectTransformStream<USStreetAddress.QueryParamsItem[][], USStreetAddress.QueryResult>(multi_normalize_query);
}