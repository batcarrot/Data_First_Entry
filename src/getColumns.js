"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
const csv = require('csv-parser');
const fs = __importStar(require("fs"));
const parquetjs_1 = require("parquetjs");
const readline = __importStar(require("readline"));
const stream_chain_1 = require("stream-chain");
const stream_json_1 = require("stream-json");
const streamValues_1 = require("stream-json/streamers/streamValues");
// Function to get columns from a JSON file
function getColumnsFromJson(filePath) {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((resolve, reject) => {
            // Create a pipeline to read the JSON file, parse it, and stream the values
            const pipeline = (0, stream_chain_1.chain)([
                fs.createReadStream(filePath),
                (0, stream_json_1.parser)(),
                (0, streamValues_1.streamValues)()
            ]);
            let columnsFound = false;
            // Listen for 'data' events from the pipeline
            pipeline.on('data', (data) => {
                // Check if the data value is an object
                if (data.value && typeof data.value === 'object') {
                    if (data.value.meta && data.value.meta.view && Array.isArray(data.value.meta.view.columns)) {
                        // Extract column names from meta.view.columns array
                        const columns = data.value.meta.view.columns
                            .filter((column) => column.id >= 0) // Check for valid non-negative IDs => columns
                            .map((column) => column.name); // Extract column names
                        pipeline.destroy();
                        resolve(columns);
                        columnsFound = true;
                        // Check if the data value is an array of objects    
                    }
                    else if (Array.isArray(data.value) && data.value.length > 0 && typeof data.value[0] === 'object') {
                        // Extract keys from the first object as column names
                        const keys = Object.keys(data.value[0]);
                        pipeline.destroy();
                        resolve(keys);
                        columnsFound = true;
                        // Check if the data key is 'data' and the value is an array of objects
                    }
                    else if (data.key === 'data' && Array.isArray(data.value) && data.value.length > 0 && typeof data.value[0] === 'object') {
                        // Extract keys from the first object as column names
                        const keys = Object.keys(data.value[0]);
                        pipeline.destroy();
                        resolve(keys);
                        columnsFound = true;
                    }
                }
            });
            pipeline.on('error', (error) => {
                reject(error);
            });
            pipeline.on('end', () => {
                if (!columnsFound) {
                    reject(new Error('No valid columns found in the JSON file'));
                }
            });
        });
    });
}
// Function to get columns from a JSONL file
function getColumnsFromJsonl(filePath) {
    var _a, e_1, _b, _c;
    return __awaiter(this, void 0, void 0, function* () {
        const fileStream = fs.createReadStream(filePath);
        const rl = readline.createInterface({ input: fileStream });
        try {
            for (var _d = true, rl_1 = __asyncValues(rl), rl_1_1; rl_1_1 = yield rl_1.next(), _a = rl_1_1.done, !_a;) {
                _c = rl_1_1.value;
                _d = false;
                try {
                    const line = _c;
                    const data = JSON.parse(line);
                    // Check for meta.view.columns structure
                    if (data.meta && data.meta.view && Array.isArray(data.meta.view.columns)) {
                        // Extract column names from meta.view.columns array
                        const columns = data.meta.view.columns
                            .filter((column) => column.id >= 0) // Check for valid non-negative IDs
                            .map((column) => column.name); // Extract column names
                        return columns;
                        // Check if the data is an array and the first element is an object
                    }
                    else if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object') {
                        // Extract keys from the first object as column names
                        return Object.keys(data[0]);
                        // Check if the data is an object
                    }
                    else if (typeof data === 'object') {
                        // Extract keys from the object as column names
                        return Object.keys(data);
                    }
                }
                finally {
                    _d = true;
                }
            }
        }
        catch (e_1_1) { e_1 = { error: e_1_1 }; }
        finally {
            try {
                if (!_d && !_a && (_b = rl_1.return)) yield _b.call(rl_1);
            }
            finally { if (e_1) throw e_1.error; }
        }
        return [];
    });
}
// Function to get columns from a CSV file
function getColumnsFromCsv(filePath, delimiter = ',') {
    return __awaiter(this, void 0, void 0, function* () {
        return new Promise((resolve, reject) => {
            const headers = [];
            fs.createReadStream(filePath)
                .pipe(csv({ separator: delimiter }))
                .on('headers', (headerList) => {
                headers.push(...headerList);
                resolve(headers);
            })
                .on('error', (error) => reject(error));
        });
    });
}
// Function to get columns from a Parquet file
function getColumnsFromParquet(filePath) {
    return __awaiter(this, void 0, void 0, function* () {
        let reader = null;
        try {
            reader = yield parquetjs_1.ParquetReader.openFile(filePath);
            const cursor = reader.getCursor();
            const firstRecord = yield cursor.next();
            // Extract the keys from the first record as column names
            return firstRecord ? Object.keys(firstRecord) : [];
        }
        catch (error) {
            throw new Error(`Failed to read Parquet file: ${error.message}`);
        }
        finally {
            if (reader) {
                yield reader.close();
            }
        }
    });
}
// File path
function getFileExtension(filePath) {
    return filePath.split('.').pop();
}
// Main function
function getColumns(filePath) {
    return __awaiter(this, void 0, void 0, function* () {
        const fileExtension = getFileExtension(filePath);
        if (fileExtension === 'json') {
            return yield getColumnsFromJson(filePath);
        }
        else if (fileExtension === 'jsonl') {
            return yield getColumnsFromJsonl(filePath);
        }
        else if (fileExtension === 'csv' || fileExtension === 'tsv') {
            return yield getColumnsFromCsv(filePath, fileExtension === 'tsv' ? '\t' : ',');
        }
        else if (fileExtension === 'parquet') {
            return yield getColumnsFromParquet(filePath);
        }
        else {
            throw new Error(`Unsupported file extension: ${fileExtension}`);
        }
    });
}
// Entry point
(() => __awaiter(void 0, void 0, void 0, function* () {
    const filePath = process.argv[2];
    if (!filePath) {
        console.error('Please provide a file path as an argument.');
        process.exit(1);
    }
    try {
        const columns = yield getColumns(filePath);
        console.log(`Columns/Keys: ${columns}`);
    }
    catch (error) {
        console.error(`Error: ${error.message}`);
    }
}))();
