/**
 * SQLite Database - A simple SQLite database reader in TypeScript
 * From CodeCrafters.io build-your-own-sqlite (TypeScript)
 */

import { open } from 'fs/promises';
import { constants } from 'fs';

const args = process.argv;
const databaseFilePath: string = args[2]
const command: string = args[3];

// Helper to read a varint from a buffer
function readVarint(buffer: Uint8Array, offset: number): [number, number] {
    let value = 0;
    let bytesRead = 0;
    
    for (let i = 0; i < 9; i++) {
        const byte = buffer[offset + i];
        bytesRead++;
        
        if (i === 8) {
            // 9th byte uses all 8 bits
            value = (value << 8) | byte;
            break;
        } else {
            value = (value << 7) | (byte & 0x7f);
            if ((byte & 0x80) === 0) {
                break;
            }
        }
    }
    
    return [value, bytesRead];
}

// Helper to get the size of a value based on its serial type
function getSerialTypeSize(serialType: number): number {
    if (serialType === 0) return 0;
    if (serialType === 1) return 1;
    if (serialType === 2) return 2;
    if (serialType === 3) return 3;
    if (serialType === 4) return 4;
    if (serialType === 5) return 6;
    if (serialType === 6) return 8;
    if (serialType === 7) return 8;
    if (serialType === 8) return 0;
    if (serialType === 9) return 0;
    if (serialType === 10 || serialType === 11) return 0;
    if (serialType >= 12 && serialType % 2 === 0) {
        // BLOB
        return (serialType - 12) / 2;
    }
    if (serialType >= 13 && serialType % 2 === 1) {
        // TEXT
        return (serialType - 13) / 2;
    }
    return 0;
}

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
} else if (command === ".tables") {
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    
    // Read page header (8 bytes for leaf page, starting at offset 100)
    const pageHeaderBuffer = new Uint8Array(8);
    await databaseFileHandler.read(pageHeaderBuffer, 0, 8, 100);
    
    const pageHeaderView = new DataView(pageHeaderBuffer.buffer);
    const numberOfCells = pageHeaderView.getUint16(3);
    
    // Read cell pointer array (starts at offset 108, 2 bytes per cell)
    const cellPointerArraySize = numberOfCells * 2;
    const cellPointerArrayBuffer = new Uint8Array(cellPointerArraySize);
    await databaseFileHandler.read(cellPointerArrayBuffer, 0, cellPointerArraySize, 108);
    
    const cellPointerArrayView = new DataView(cellPointerArrayBuffer.buffer);
    const tableNames: string[] = [];
    
    // Read each cell
    for (let i = 0; i < numberOfCells; i++) {
        const cellOffset = cellPointerArrayView.getUint16(i * 2);
        
        const cellBuffer = new Uint8Array(1000);
        await databaseFileHandler.read(cellBuffer, 0, cellBuffer.length, cellOffset);
        
        let offset = 0;
        const [recordSize, recordSizeBytes] = readVarint(cellBuffer, offset);
        offset += recordSizeBytes;
        
        const [rowid, rowidBytes] = readVarint(cellBuffer, offset);
        offset += rowidBytes;
        
        const [headerSize, headerSizeBytes] = readVarint(cellBuffer, offset);
        offset += headerSizeBytes;
        
        // Read serial types for columns
        const serialTypes: number[] = [];
        const headerStart = offset - headerSizeBytes;
        while (offset - headerStart < headerSize) {
            const [serialType, serialTypeBytes] = readVarint(cellBuffer, offset);
            serialTypes.push(serialType);
            offset += serialTypeBytes;
        }
        
        const typeSize = getSerialTypeSize(serialTypes[0]);
        offset += typeSize;
        
        const nameSize = getSerialTypeSize(serialTypes[1]);
        offset += nameSize;
        
        const tblNameSize = getSerialTypeSize(serialTypes[2]);
        const tblName = new TextDecoder().decode(cellBuffer.slice(offset, offset + tblNameSize));
        tableNames.push(tblName);
    }
    
    console.log(tableNames.join(' '));
    
    await databaseFileHandler.close();
} else {
    throw new Error(`Unknown command ${command}`);
}
