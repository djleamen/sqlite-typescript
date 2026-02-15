/**
 * SQLite Database Reader in TypeScript
 * A simple implementation for reading and querying SQLite database files.
 * From CodeCrafters.io build-your-own-sqlite (TypeScript)
 */

import { open } from 'fs/promises';
import { constants } from 'fs';
import { handleDbInfo } from './commands/dbinfo.js';
import { handleTables } from './commands/tables.js';
import { handleSelectCount, handleSelect } from './commands/select.js';

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];
const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);

try {
    if (command === ".dbinfo") {
        await handleDbInfo(databaseFileHandler);
    } else if (command === ".tables") {
        await handleTables(databaseFileHandler);
    } else if (command.toUpperCase().startsWith("SELECT COUNT(*) FROM")) {
        const tableName = command.split(' ').pop()!;
        
        // Read page size
        const headerBuffer = new Uint8Array(100);
        await databaseFileHandler.read(headerBuffer, 0, 100, 0);
        const pageSize = new DataView(headerBuffer.buffer).getUint16(16);
        
        await handleSelectCount(databaseFileHandler, pageSize, tableName);
    } else if (command.toUpperCase().startsWith("SELECT") && !command.toUpperCase().includes("COUNT(*)")) {
        // Read page size
        const headerBuffer = new Uint8Array(100);
        await databaseFileHandler.read(headerBuffer, 0, 100, 0);
        const pageSize = new DataView(headerBuffer.buffer).getUint16(16);
        
        await handleSelect(databaseFileHandler, pageSize, command);
    } else {
        throw new Error(`Unknown command ${command}`);
    }
} finally {
    await databaseFileHandler.close();
}
