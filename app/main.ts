/**
 * SQLite Database - A simple SQLite database reader in TypeScript
 * From CodeCrafters.io build-your-own-sqlite (TypeScript)
 */

import { open } from 'fs/promises';
import type { FileHandle } from 'fs/promises';
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

// Parse a record and return column values
function parseRecord(buffer: Uint8Array): string[] {
    let offset = 0;
    
    // Skip record size
    const [recordSize, recordSizeBytes] = readVarint(buffer, offset);
    offset += recordSizeBytes;
    
    // Skip rowid
    const [rowid, rowidBytes] = readVarint(buffer, offset);
    offset += rowidBytes;
    
    // Read header
    const [headerSize, headerSizeBytes] = readVarint(buffer, offset);
    offset += headerSizeBytes;
    
    // Read serial types
    const serialTypes: number[] = [];
    const headerStart = offset - headerSizeBytes;
    while (offset - headerStart < headerSize) {
        const [serialType, serialTypeBytes] = readVarint(buffer, offset);
        serialTypes.push(serialType);
        offset += serialTypeBytes;
    }
    
    // Read column values
    const values: string[] = [];
    for (const serialType of serialTypes) {
        const size = getSerialTypeSize(serialType);
        const value = new TextDecoder().decode(buffer.slice(offset, offset + size));
        values.push(value);
        offset += size;
    }
    
    return values;
}

// Read page header and return cell count
async function readPageHeader(fileHandler: FileHandle, pageOffset: number, isPage1: boolean): Promise<number> {
    const headerOffset = isPage1 ? 100 : 0;
    const buffer = new Uint8Array(8);
    await fileHandler.read(buffer, 0, 8, pageOffset + headerOffset);
    return new DataView(buffer.buffer).getUint16(3);
}

// Read cell pointer array
async function readCellPointers(fileHandler: FileHandle, pageOffset: number, isPage1: boolean, cellCount: number): Promise<number[]> {
    const headerOffset = isPage1 ? 100 : 0;
    const arraySize = cellCount * 2;
    const buffer = new Uint8Array(arraySize);
    await fileHandler.read(buffer, 0, arraySize, pageOffset + headerOffset + 8);
    
    const view = new DataView(buffer.buffer);
    const pointers: number[] = [];
    for (let i = 0; i < cellCount; i++) {
        pointers.push(view.getUint16(i * 2));
    }
    return pointers;
}

// Read a cell from a page
async function readCell(fileHandler: FileHandle, pageOffset: number, cellOffset: number): Promise<Uint8Array> {
    const buffer = new Uint8Array(2000);
    await fileHandler.read(buffer, 0, buffer.length, pageOffset + cellOffset);
    return buffer;
}

// Find table in sqlite_schema and return rootpage and optionally CREATE TABLE SQL
async function findTable(fileHandler: FileHandle, tableName: string, includeSql: boolean = false): Promise<{ rootPage: number, sql?: string }> {
    const cellCount = await readPageHeader(fileHandler, 0, true);
    const cellPointers = await readCellPointers(fileHandler, 0, true, cellCount);
    
    for (const cellOffset of cellPointers) {
        const cellBuffer = await readCell(fileHandler, 0, cellOffset);
        const values = parseRecord(cellBuffer);
        
        // values: [type, name, tbl_name, rootpage, sql]
        if (values[2] === tableName) {
            const result: { rootPage: number, sql?: string } = {
                rootPage: values[3].charCodeAt(0)
            };
            if (includeSql) {
                result.sql = values[4];
            }
            return result;
        }
    }
    
    throw new Error(`Table ${tableName} not found`);
}

// Read all cells from a table page
async function readTableCells(fileHandler: FileHandle, pageSize: number, rootPage: number): Promise<string[][]> {
    const pageOffset = (rootPage - 1) * pageSize;
    const isPage1 = rootPage === 1;
    
    const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
    const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount);
    
    const rows: string[][] = [];
    for (const cellOffset of cellPointers) {
        const cellBuffer = await readCell(fileHandler, pageOffset, cellOffset);
        rows.push(parseRecord(cellBuffer));
    }
    
    return rows;
}

// Parse CREATE TABLE to extract column names
function parseCreateTable(sql: string): string[] {
    const columnsMatch = sql.match(/\(([^)]+)\)/s);
    if (!columnsMatch) {
        throw new Error("Failed to parse CREATE TABLE statement");
    }
    
    const columnsText = columnsMatch[1];
    const columnDefs = columnsText.split(',').map(col => col.trim());
    return columnDefs.map(def => def.split(/\s+/)[0].trim());
}

if (command === ".dbinfo") {
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    const buffer: Uint8Array = new Uint8Array(100);
    await databaseFileHandler.read(buffer, 0, buffer.length, 0);

    console.error("Logs from your program will appear here!");

    const pageSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(16);
    console.log(`database page size: ${pageSize}`);

    const numberOfTables = await readPageHeader(databaseFileHandler, 0, true);
    console.log(`number of tables: ${numberOfTables}`);

    await databaseFileHandler.close();
    
} else if (command === ".tables") {
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    
    const cellCount = await readPageHeader(databaseFileHandler, 0, true);
    const cellPointers = await readCellPointers(databaseFileHandler, 0, true, cellCount);
    
    const tableNames: string[] = [];
    for (const cellOffset of cellPointers) {
        const cellBuffer = await readCell(databaseFileHandler, 0, cellOffset);
        const values = parseRecord(cellBuffer);
        tableNames.push(values[2]); // tbl_name is column 2
    }
    
    console.log(tableNames.join(' '));
    await databaseFileHandler.close();

} else if (command.toUpperCase().startsWith("SELECT COUNT(*) FROM")) {
    const tableName = command.split(' ').pop()!;
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    
    // Read page size
    const headerBuffer = new Uint8Array(100);
    await databaseFileHandler.read(headerBuffer, 0, 100, 0);
    const pageSize = new DataView(headerBuffer.buffer).getUint16(16);
    
    // Find table and get root page
    const { rootPage } = await findTable(databaseFileHandler, tableName);
    
    // Count cells on the root page
    const pageOffset = (rootPage - 1) * pageSize;
    const isPage1 = rootPage === 1;
    const rowCount = await readPageHeader(databaseFileHandler, pageOffset, isPage1);
    
    console.log(rowCount);
    await databaseFileHandler.close();

} else if (command.toUpperCase().startsWith("SELECT") && !command.toUpperCase().includes("COUNT(*)")) {
    const parts = command.split(/\s+/);
    const selectIndex = parts.findIndex(p => p.toUpperCase() === "SELECT");
    const fromIndex = parts.findIndex(p => p.toUpperCase() === "FROM");
    
    // Extract column names (may be comma-separated)
    const columnsStr = parts.slice(selectIndex + 1, fromIndex).join(' ');
    const columnNames = columnsStr.split(',').map(c => c.trim());
    const tableName = parts[fromIndex + 1];
    
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    
    // Read page size
    const headerBuffer = new Uint8Array(100);
    await databaseFileHandler.read(headerBuffer, 0, 100, 0);
    const pageSize = new DataView(headerBuffer.buffer).getUint16(16);
    
    // Find table and get CREATE TABLE SQL
    const { rootPage, sql } = await findTable(databaseFileHandler, tableName, true);
    
    // Parse CREATE TABLE to find column indices
    const columns = parseCreateTable(sql!);
    const columnIndices = columnNames.map(name => {
        const index = columns.indexOf(name);
        if (index === -1) {
            throw new Error(`Column ${name} not found in table ${tableName}`);
        }
        return index;
    });
    
    // Read all rows from the table
    const rows = await readTableCells(databaseFileHandler, pageSize, rootPage);
    
    // Extract and print the requested columns
    rows.forEach(row => {
        const values = columnIndices.map(idx => row[idx]);
        console.log(values.join('|'));
    });
    
    await databaseFileHandler.close();

} else {
    throw new Error(`Unknown command ${command}`);
}
