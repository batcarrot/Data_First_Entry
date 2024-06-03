const csv = require('csv-parser');
import * as fs from 'fs';
import { ParquetReader } from 'parquetjs';
import * as readline from 'readline';
import { chain } from 'stream-chain';
import { parser } from 'stream-json';
import { streamValues } from 'stream-json/streamers/streamValues';

// Function to get columns from a JSON file
async function getColumnsFromJson(filePath: string): Promise<string[]> {
    return new Promise<string[]>((resolve, reject) => {
        // Create a pipeline to read the JSON file, parse it, and stream the values
        const pipeline = chain([
            fs.createReadStream(filePath),
            parser(),
            streamValues()
        ]);

        let columnsFound = false;

        // Listen for 'data' events from the pipeline
        pipeline.on('data', (data: any) => {
            // Check if the data value is an object
            if (data.value && typeof data.value === 'object') {
                if (data.value.meta && data.value.meta.view && Array.isArray(data.value.meta.view.columns)) {
                    // Extract column names from meta.view.columns array
                    const columns = data.value.meta.view.columns
                        .filter((column: any) => column.id >= 0) // Check for valid non-negative IDs => columns
                        .map((column: any) => column.name); // Extract column names

                    pipeline.destroy();
                    resolve(columns);
                    columnsFound = true;

                // Check if the data value is an array of objects    
                } else if (Array.isArray(data.value) && data.value.length > 0 && typeof data.value[0] === 'object') {
                    // Extract keys from the first object as column names
                    const keys = Object.keys(data.value[0]);
                    pipeline.destroy();
                    resolve(keys);
                    columnsFound = true;
                // Check if the data key is 'data' and the value is an array of objects
                } else if (data.key === 'data' && Array.isArray(data.value) && data.value.length > 0 && typeof data.value[0] === 'object') {
                    // Extract keys from the first object as column names
                    const keys = Object.keys(data.value[0]);
                    pipeline.destroy();
                    resolve(keys);
                    columnsFound = true;
                }
            }
        });

        pipeline.on('error', (error: Error) => {
            reject(error);
        });

        pipeline.on('end', () => {
            if (!columnsFound) {
                reject(new Error('No valid columns found in the JSON file'));
            }
        });
    });
}

// Function to get columns from a JSONL file
async function getColumnsFromJsonl(filePath: string): Promise<string[]> {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({ input: fileStream });
    for await (const line of rl) {
        const data = JSON.parse(line);
        // Check for meta.view.columns structure
        if (data.meta && data.meta.view && Array.isArray(data.meta.view.columns)) {
            // Extract column names from meta.view.columns array
            const columns = data.meta.view.columns
                .filter((column: any) => column.id >= 0) // Check for valid non-negative IDs
                .map((column: any) => column.name); // Extract column names
            return columns;
        // Check if the data is an array and the first element is an object
        } else if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object') {
            // Extract keys from the first object as column names
            return Object.keys(data[0]);
        // Check if the data is an object
        } else if (typeof data === 'object') {
            // Extract keys from the object as column names
            return Object.keys(data);
        }
    }
    return [];
}

// Function to get columns from a CSV file
async function getColumnsFromCsv(filePath: string, delimiter: string = ','): Promise<string[]> {
    return new Promise((resolve, reject) => {
        const headers: string[] = [];
        fs.createReadStream(filePath)
            .pipe(csv({ separator: delimiter }))
            .on('headers', (headerList: string[]) => {
                headers.push(...headerList);
                resolve(headers);
            })
            .on('error', (error: Error) => reject(error));
    });
}

// Function to get columns from a Parquet file
async function getColumnsFromParquet(filePath: string): Promise<string[]> {
    let reader: ParquetReader | null = null;
    try {
        reader = await ParquetReader.openFile(filePath);
        const cursor = reader.getCursor();
        const firstRecord = await cursor.next();
        // Extract the keys from the first record as column names
        return firstRecord ? Object.keys(firstRecord) : [];
    } catch (error) {
        throw new Error(`Failed to read Parquet file: ${(error as Error).message}`);
    } finally {
        if (reader) {
            await reader.close();
        }
    }
}

// File path
function getFileExtension(filePath: string): string {
    return filePath.split('.').pop()!;
}

// Main function
async function getColumns(filePath: string): Promise<string[]> {
    const fileExtension = getFileExtension(filePath);
    if (fileExtension === 'json') {
        return await getColumnsFromJson(filePath);
    } else if (fileExtension === 'jsonl') {
        return await getColumnsFromJsonl(filePath);
    } else if (fileExtension === 'csv' || fileExtension === 'tsv') {
        return await getColumnsFromCsv(filePath, fileExtension === 'tsv' ? '\t' : ',');
    } else if (fileExtension === 'parquet') {
        return await getColumnsFromParquet(filePath);
    } else {
        throw new Error(`Unsupported file extension: ${fileExtension}`);
    }
}

// Entry point
(async () => {
    const filePath = process.argv[2];
    if (!filePath) {
        console.error('Please provide a file path as an argument.');
        process.exit(1);
    }

    try {
        const columns = await getColumns(filePath);
        console.log(`Columns/Keys: ${columns}`);
    } catch (error) {
        console.error(`Error: ${(error as Error).message}`);
    }
})();