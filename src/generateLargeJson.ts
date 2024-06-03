import * as fs from 'fs';
import * as path from 'path';

interface DataItem {
  index: number;
  value: string;
}

const filePath = path.join(__dirname, 'large-file.json');
const targetSizeInBytes = 1024 * 1024 * 1024; // 1 GB

const generateData = (size: number): DataItem[] => {
  const data: DataItem[] = [];
  for (let i = 0; i < size; i++) {
    data.push({ index: i, value: Math.random().toString(36).substring(2, 15) });
  }
  return data;
};

const writeFile = async (filePath: string, data: string) => {
  return new Promise<void>((resolve, reject) => {
    fs.writeFile(filePath, data, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
};

const generateLargeFile = async (filePath: string, targetSizeInBytes: number) => {
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
};

generateLargeFile(filePath, targetSizeInBytes);