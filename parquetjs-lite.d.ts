declare module 'parquetjs-lite' {
    export class ParquetReader {
      static openFile(filePath: string): Promise<ParquetReader>;
      getCursor(): ParquetCursor;
      close(): Promise<void>;
    }
  
    export class ParquetCursor {
      next(): Promise<Record<string, any> | null>;
    }
  }
  