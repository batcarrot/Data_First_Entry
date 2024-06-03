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
Object.defineProperty(exports, "__esModule", { value: true });
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const filePath = path.join(__dirname, 'large-file.json');
const targetSizeInBytes = 1024 * 1024 * 1024; // 1 GB
const generateData = (size) => {
    const data = [];
    for (let i = 0; i < size; i++) {
        data.push({ index: i, value: Math.random().toString(36).substring(2, 15) });
    }
    return data;
};
const writeFile = (filePath, data) => __awaiter(void 0, void 0, void 0, function* () {
    return new Promise((resolve, reject) => {
        fs.writeFile(filePath, data, (err) => {
            if (err) {
                reject(err);
            }
            else {
                resolve();
            }
        });
    });
});
const generateLargeFile = (filePath, targetSizeInBytes) => __awaiter(void 0, void 0, void 0, function* () {
    let dataSize = 0;
    const writeStream = fs.createWriteStream(filePath);
    while (dataSize < targetSizeInBytes) {
        const chunkSize = Math.min(1024 * 1024, targetSizeInBytes - dataSize); // Limit chunk size to 1 MB
        const chunk = JSON.stringify(generateData(chunkSize));
        writeStream.write(chunk);
        dataSize += Buffer.byteLength(chunk, 'utf8');
        console.log(`Generated ${dataSize} bytes`);
    }
    writeStream.end();
    console.log(`File ${filePath} written successfully (${dataSize} bytes)`);
});
generateLargeFile(filePath, targetSizeInBytes);
