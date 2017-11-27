import {ObjectTransformStream} from "object-transform-stream";
import {Transform} from "stream";
import {USStreetAddress} from "smartystreets-types";
import * as request from "request";
import JSONStream = require('JSONStream');

export interface NormalizationResult {
    RequestCount: number;
    QueryResult: USStreetAddress.QueryResult
}

export function normalize_query(query: USStreetAddress.QueryParamsItem[]) : Promise<NormalizationResult> {
    return new Promise<NormalizationResult>((resolve: (value: NormalizationResult) => void, reject: (err: any) => void) => {
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
            resolve({RequestCount:query.length, QueryResult: result});
        });
        request.post(url, {json: query}).pipe(jsonParse);
    });
}

export function normalize(): Transform {
    return new ObjectTransformStream<USStreetAddress.QueryParamsItem[], NormalizationResult>(normalize_query);
}

export function concurrent_normalize_query(queries: USStreetAddress.QueryParamsItem[][]) : Promise<NormalizationResult> {
    let promises:Promise<NormalizationResult>[] = [];
    let RequestCount = 0;
    for (let i in queries) {
        let query = queries[i];
        RequestCount += query.length;
        promises.push(normalize_query(query));
    }
    let p = Promise.all(promises);  
    return p.then((value: NormalizationResult[]) => {
        let ret : NormalizationResult = {RequestCount, QueryResult: []};
        for (let i in value) {
            for (let j in value[i].QueryResult) {
                let item = value[i].QueryResult[j];
                ret.QueryResult.push(item);
            }
        }
        return ret;
    });
}

export function concurrent_normalize(): Transform {
    return new ObjectTransformStream<USStreetAddress.QueryParamsItem[][], NormalizationResult>(concurrent_normalize_query);
}