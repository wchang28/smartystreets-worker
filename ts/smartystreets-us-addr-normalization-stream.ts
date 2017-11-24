import {ObjectTransformStream} from "object-transform-stream";
import {Transform} from "stream";
import {USStreetAddress} from "smartystreets-types";
import * as request from "request";
import JSONStream = require('JSONStream');

let AUTH_ID = process.env["SMARTYSTREETS_AUTH_ID"];
let AUTH_TOKEN = process.env["SMARTYSTREETS_AUTH_TOKEN"];

function transformer(query: USStreetAddress.QueryParamsItem[]) : Promise<USStreetAddress.QueryResult> {
    return new Promise<USStreetAddress.QueryResult>((resolve: (value: USStreetAddress.QueryResult) => void, reject: (err: any) => void) => {
        let url = "https://us-street.api.smartystreets.com/street-address?auth-id=" + AUTH_ID +"&auth-token=" + AUTH_TOKEN;
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
    return new ObjectTransformStream<USStreetAddress.QueryParamsItem[], USStreetAddress.QueryResult>(transformer);
}