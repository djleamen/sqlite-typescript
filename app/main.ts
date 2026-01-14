import { open } from 'fs/promises';
import { constants } from 'fs';

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];

if (command === ".dbinfo") {
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    const buffer: Uint8Array = new Uint8Array(100);
    await databaseFileHandler.read(buffer, 0, buffer.length, 0);

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    console.error("Logs from your program will appear here!");

    const pageSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(16);
    console.log(`database page size: ${pageSize}`);

    // Read the page header (starts at offset 100) to get the number of cells 
    // (cell count is at bytes 3-4 of the page header)
    const pageHeaderBuffer: Uint8Array = new Uint8Array(5);
    await databaseFileHandler.read(pageHeaderBuffer, 0, pageHeaderBuffer.length, 100);
    const numberOfTables = new DataView(pageHeaderBuffer.buffer, 0, pageHeaderBuffer.byteLength).getUint16(3);
    console.log(`number of tables: ${numberOfTables}`);

    await databaseFileHandler.close();
} else {
    throw new Error(`Unknown command ${command}`);
}
