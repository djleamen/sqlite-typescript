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

// Parse a record and return column values (also return rowid)
function parseRecord(buffer: Uint8Array): { rowid: number, values: string[] } {
    let offset = 0;
    
    // Skip record size
    const [recordSize, recordSizeBytes] = readVarint(buffer, offset);
    offset += recordSizeBytes;
    
    // Read rowid (we'll need this)
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
        
        // Handle different data types based on serial type
        if (serialType === 0) {
            // NULL
            values.push('');
        } else if (serialType === 8) {
            // Integer constant 0
            values.push('0');
        } else if (serialType === 9) {
            // Integer constant 1
            values.push('1');
        } else if (serialType >= 1 && serialType <= 6) {
            // Integer (1-6 bytes) - SQLite uses big-endian twos-complement
            const view = new DataView(buffer.buffer, buffer.byteOffset + offset, size);
            let intValue = 0;
            
            if (size === 1) {
                intValue = view.getInt8(0);
            } else if (size === 2) {
                intValue = view.getInt16(0, false); // big-endian
            } else if (size === 3) {
                // 24-bit signed integer
                const byte0 = buffer[offset];
                const byte1 = buffer[offset + 1];
                const byte2 = buffer[offset + 2];
                intValue = (byte0 << 16) | (byte1 << 8) | byte2;
                // Sign extend if negative
                if (intValue & 0x800000) {
                    intValue |= 0xFF000000;
                }
            } else if (size === 4) {
                intValue = view.getInt32(0, false); // big-endian
            } else if (size === 6) {
                // 48-bit signed integer
                const high = view.getInt16(0, false);
                const low = view.getUint32(2, false);
                intValue = (high * 0x100000000) + low;
            } else if (size === 8) {
                intValue = Number(view.getBigInt64(0, false));
            }
            
            values.push(intValue.toString());
            offset += size;
        } else if (serialType === 7) {
            // Float
            const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 8);
            const floatValue = view.getFloat64(0, false); // big-endian
            values.push(floatValue.toString());
            offset += size;
        } else {
            // TEXT or BLOB - decode as text
            const value = new TextDecoder().decode(buffer.slice(offset, offset + size));
            values.push(value);
            offset += size;
        }
    }
    
    return { rowid, values };
}

// Read page header and return cell count
async function readPageHeader(fileHandler: FileHandle, pageOffset: number, isPage1: boolean): Promise<number> {
    const headerOffset = isPage1 ? 100 : 0;
    const buffer = new Uint8Array(12);
    await fileHandler.read(buffer, 0, 12, pageOffset + headerOffset);
    return new DataView(buffer.buffer).getUint16(3);
}

// Read page type from page header
async function readPageType(fileHandler: FileHandle, pageOffset: number, isPage1: boolean): Promise<number> {
    const headerOffset = isPage1 ? 100 : 0;
    const buffer = new Uint8Array(1);
    await fileHandler.read(buffer, 0, 1, pageOffset + headerOffset);
    return buffer[0];
}

// Read rightmost pointer from interior page header
async function readRightmostPointer(fileHandler: FileHandle, pageOffset: number, isPage1: boolean): Promise<number> {
    const headerOffset = isPage1 ? 100 : 0;
    const buffer = new Uint8Array(12);
    await fileHandler.read(buffer, 0, 12, pageOffset + headerOffset);
    return new DataView(buffer.buffer).getUint32(8);
}

// Read cell pointer array
async function readCellPointers(fileHandler: FileHandle, pageOffset: number, isPage1: boolean, cellCount: number, isInterior: boolean = false): Promise<number[]> {
    const headerOffset = isPage1 ? 100 : 0;
    // Interior pages have 4-byte rightmost pointer at offset 8-11, so cell array starts at 12
    const cellArrayOffset = isInterior ? 12 : 8;
    const arraySize = cellCount * 2;
    const buffer = new Uint8Array(arraySize);
    await fileHandler.read(buffer, 0, arraySize, pageOffset + headerOffset + cellArrayOffset);
    
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
        const { values } = parseRecord(cellBuffer);
        
        // values: [type, name, tbl_name, rootpage, sql]
        if (values[2] === tableName) {
            const result: { rootPage: number, sql?: string } = {
                rootPage: parseInt(values[3])
            };
            if (includeSql) {
                result.sql = values[4];
            }
            return result;
        }
    }
    
    throw new Error(`Table ${tableName} not found`);
}

// Find index in sqlite_schema and return rootpage
async function findIndex(fileHandler: FileHandle, indexName: string): Promise<number> {
    const cellCount = await readPageHeader(fileHandler, 0, true);
    const cellPointers = await readCellPointers(fileHandler, 0, true, cellCount);
    
    for (const cellOffset of cellPointers) {
        const cellBuffer = await readCell(fileHandler, 0, cellOffset);
        const { values } = parseRecord(cellBuffer);
        
        // values: [type, name, tbl_name, rootpage, sql]
        if (values[0] === 'index' && values[1] === indexName) {
            return parseInt(values[3]);
        }
    }
    
    throw new Error(`Index ${indexName} not found`);
}

// Read all cells from a table page
async function readTableCells(fileHandler: FileHandle, pageSize: number, rootPage: number): Promise<Array<{ rowid: number, values: string[] }>> {
    const rows: Array<{ rowid: number, values: string[] }> = [];
    await readTableCellsRecursive(fileHandler, pageSize, rootPage, rows);
    return rows;
}

// Read cells from a table page, filtering by rowid set
async function readTableCellsFiltered(fileHandler: FileHandle, pageSize: number, rootPage: number, rowidSet: Set<number>, rows: Array<{ rowid: number, values: string[] }>): Promise<void> {
    await readTableCellsFilteredRecursive(fileHandler, pageSize, rootPage, rowidSet, rows);
}

// Recursive helper to traverse B-tree and collect filtered rows
async function readTableCellsFilteredRecursive(fileHandler: FileHandle, pageSize: number, pageNum: number, rowidSet: Set<number>, rows: Array<{ rowid: number, values: string[] }>): Promise<void> {
    const pageOffset = (pageNum - 1) * pageSize;
    const isPage1 = pageNum === 1;
    
    const pageType = await readPageType(fileHandler, pageOffset, isPage1);
    
    if (pageType === 0x0d) {
        // Leaf page - read cells and filter
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, false);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = await readCell(fileHandler, pageOffset, cellOffset);
            const record = parseRecord(cellBuffer);
            
            if (rowidSet.has(record.rowid)) {
                rows.push(record);
            }
        }
    } else if (pageType === 0x05) {
        // Interior page - traverse children
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, true);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = new Uint8Array(16);
            await fileHandler.read(cellBuffer, 0, 16, pageOffset + cellOffset);
            
            const leftChildPage = new DataView(cellBuffer.buffer).getUint32(0, false);
            await readTableCellsFilteredRecursive(fileHandler, pageSize, leftChildPage, rowidSet, rows);
        }
        
        const rightmostChild = await readRightmostPointer(fileHandler, pageOffset, isPage1);
        await readTableCellsFilteredRecursive(fileHandler, pageSize, rightmostChild, rowidSet, rows);
    }
}

// Recursive helper to traverse B-tree and collect all rows
async function readTableCellsRecursive(fileHandler: FileHandle, pageSize: number, pageNum: number, rows: Array<{ rowid: number, values: string[] }>): Promise<void> {
    const pageOffset = (pageNum - 1) * pageSize;
    const isPage1 = pageNum === 1;
    
    // Check page type (0x0d = leaf, 0x05 = interior)
    const pageType = await readPageType(fileHandler, pageOffset, isPage1);
    
    if (pageType === 0x0d) {
        // Leaf page - read all cells
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, false);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = await readCell(fileHandler, pageOffset, cellOffset);
            const record = parseRecord(cellBuffer);
            rows.push(record);
        }
    } else if (pageType === 0x05) {
        // Interior page - traverse children
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, true);
        
        // Read each cell to get left child pointers
        for (const cellOffset of cellPointers) {
            const cellBuffer = new Uint8Array(16);
            await fileHandler.read(cellBuffer, 0, 16, pageOffset + cellOffset);
            
            // First 4 bytes of interior cell is the left child page number (big-endian)
            const leftChildPage = new DataView(cellBuffer.buffer).getUint32(0, false); // false = big-endian
            await readTableCellsRecursive(fileHandler, pageSize, leftChildPage, rows);
        }
        
        // Read the rightmost child pointer
        const rightmostChild = await readRightmostPointer(fileHandler, pageOffset, isPage1);
        await readTableCellsRecursive(fileHandler, pageSize, rightmostChild, rows);
    }
}

// Scan index for matching values and return rowids
async function scanIndex(fileHandler: FileHandle, pageSize: number, rootPage: number, searchValue: string): Promise<number[]> {
    const rowids: number[] = [];
    await scanIndexRecursive(fileHandler, pageSize, rootPage, searchValue, rowids);
    return rowids;
}

// Recursive helper to traverse index B-tree and find matching entries
async function scanIndexRecursive(fileHandler: FileHandle, pageSize: number, pageNum: number, searchValue: string, rowids: number[]): Promise<void> {
    const pageOffset = (pageNum - 1) * pageSize;
    const isPage1 = pageNum === 1;
    
    // Check page type (0x0a = index leaf, 0x02 = index interior)
    const pageType = await readPageType(fileHandler, pageOffset, isPage1);
    
    if (pageType === 0x0a) {
        // Index leaf page - read all cells and check for matches
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, false);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = await readCell(fileHandler, pageOffset, cellOffset);
            
            // Parse index entry: it's a record with indexed value(s) + rowid
            let offset = 0;
            
            // Read payload size
            const [payloadSize, payloadSizeBytes] = readVarint(cellBuffer, offset);
            offset += payloadSizeBytes;
            
            // Read header size
            const [headerSize, headerSizeBytes] = readVarint(cellBuffer, offset);
            offset += headerSizeBytes;
            
            // Read serial types
            const serialTypes: number[] = [];
            const headerStart = offset - headerSizeBytes;
            while (offset - headerStart < headerSize) {
                const [serialType, serialTypeBytes] = readVarint(cellBuffer, offset);
                serialTypes.push(serialType);
                offset += serialTypeBytes;
            }
            
            // Read first value (indexed column) to check if it matches
            if (serialTypes.length >= 2) {
                const firstSerialType = serialTypes[0];
                const size = getSerialTypeSize(firstSerialType);
                
                let indexedValue = '';
                if (firstSerialType === 0) {
                    indexedValue = '';
                } else if (firstSerialType === 8) {
                    indexedValue = '0';
                } else if (firstSerialType === 9) {
                    indexedValue = '1';
                } else if (firstSerialType >= 1 && firstSerialType <= 6) {
                    const view = new DataView(cellBuffer.buffer, cellBuffer.byteOffset + offset, size);
                    let intValue = 0;
                    
                    if (size === 1) {
                        intValue = view.getInt8(0);
                    } else if (size === 2) {
                        intValue = view.getInt16(0, false);
                    } else if (size === 3) {
                        const byte0 = cellBuffer[offset];
                        const byte1 = cellBuffer[offset + 1];
                        const byte2 = cellBuffer[offset + 2];
                        intValue = (byte0 << 16) | (byte1 << 8) | byte2;
                        if (intValue & 0x800000) {
                            intValue |= 0xFF000000;
                        }
                    } else if (size === 4) {
                        intValue = view.getInt32(0, false);
                    } else if (size === 6) {
                        const high = view.getInt16(0, false);
                        const low = view.getUint32(2, false);
                        intValue = (high * 0x100000000) + low;
                    } else if (size === 8) {
                        intValue = Number(view.getBigInt64(0, false));
                    }
                    
                    indexedValue = intValue.toString();
                } else if (firstSerialType === 7) {
                    const view = new DataView(cellBuffer.buffer, cellBuffer.byteOffset + offset, 8);
                    const floatValue = view.getFloat64(0, false);
                    indexedValue = floatValue.toString();
                } else {
                    // TEXT or BLOB
                    indexedValue = new TextDecoder().decode(cellBuffer.slice(offset, offset + size));
                }
                
                // Only extract rowid if the indexed value matches
                if (indexedValue === searchValue) {
                    // Skip to the last serial type to get rowid
                    offset += size;
                    for (let i = 1; i < serialTypes.length - 1; i++) {
                        offset += getSerialTypeSize(serialTypes[i]);
                    }
                    
                    // Read rowid (last value)
                    const rowidSerialType = serialTypes[serialTypes.length - 1];
                    const rowidSize = getSerialTypeSize(rowidSerialType);
                    
                    if (rowidSerialType >= 1 && rowidSerialType <= 6) {
                        const view = new DataView(cellBuffer.buffer, cellBuffer.byteOffset + offset, rowidSize);
                        let rowid = 0;
                        
                        if (rowidSize === 1) {
                            rowid = view.getInt8(0);
                        } else if (rowidSize === 2) {
                            rowid = view.getInt16(0, false);
                        } else if (rowidSize === 3) {
                            const byte0 = cellBuffer[offset];
                            const byte1 = cellBuffer[offset + 1];
                            const byte2 = cellBuffer[offset + 2];
                            rowid = (byte0 << 16) | (byte1 << 8) | byte2;
                            if (rowid & 0x800000) {
                                rowid |= 0xFF000000;
                            }
                        } else if (rowidSize === 4) {
                            rowid = view.getInt32(0, false);
                        } else if (rowidSize === 6) {
                            const high = view.getInt16(0, false);
                            const low = view.getUint32(2, false);
                            rowid = (high * 0x100000000) + low;
                        } else if (rowidSize === 8) {
                            rowid = Number(view.getBigInt64(0, false));
                        }
                        
                        rowids.push(rowid);
                    }
                }
            }
        }
    } else if (pageType === 0x02) {
        // Index interior page - optimize by only traversing relevant subtrees
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, true);
        
        let foundGreaterKey = false;
        
        // Read each cell and check the key to decide which children to traverse
        for (const cellOffset of cellPointers) {
            const cellBuffer = new Uint8Array(1000);
            await fileHandler.read(cellBuffer, 0, cellBuffer.length, pageOffset + cellOffset);
            
            const leftChildPage = new DataView(cellBuffer.buffer).getUint32(0, false);
            
            // Parse the key (first indexed value) from this interior cell
            let offset = 4; // Skip left child pointer
            
            // Read payload size
            const [payloadSize, payloadSizeBytes] = readVarint(cellBuffer, offset);
            offset += payloadSizeBytes;
            
            // Read header size
            const [headerSize, headerSizeBytes] = readVarint(cellBuffer, offset);
            offset += headerSizeBytes;
            
            // Read serial types
            const serialTypes: number[] = [];
            const headerStart = offset - headerSizeBytes;
            while (offset - headerStart < headerSize) {
                const [serialType, serialTypeBytes] = readVarint(cellBuffer, offset);
                serialTypes.push(serialType);
                offset += serialTypeBytes;
            }
            
            // Read first value (the key)
            if (serialTypes.length >= 1) {
                const firstSerialType = serialTypes[0];
                const size = getSerialTypeSize(firstSerialType);
                
                let keyValue = '';
                if (firstSerialType === 0) {
                    keyValue = '';
                } else if (firstSerialType === 8) {
                    keyValue = '0';
                } else if (firstSerialType === 9) {
                    keyValue = '1';
                } else if (firstSerialType >= 1 && firstSerialType <= 6) {
                    const view = new DataView(cellBuffer.buffer, cellBuffer.byteOffset + offset, size);
                    let intValue = 0;
                    
                    if (size === 1) {
                        intValue = view.getInt8(0);
                    } else if (size === 2) {
                        intValue = view.getInt16(0, false);
                    } else if (size === 3) {
                        const byte0 = cellBuffer[offset];
                        const byte1 = cellBuffer[offset + 1];
                        const byte2 = cellBuffer[offset + 2];
                        intValue = (byte0 << 16) | (byte1 << 8) | byte2;
                        if (intValue & 0x800000) {
                            intValue |= 0xFF000000;
                        }
                    } else if (size === 4) {
                        intValue = view.getInt32(0, false);
                    } else if (size === 6) {
                        const high = view.getInt16(0, false);
                        const low = view.getUint32(2, false);
                        intValue = (high * 0x100000000) + low;
                    } else if (size === 8) {
                        intValue = Number(view.getBigInt64(0, false));
                    }
                    
                    keyValue = intValue.toString();
                } else if (firstSerialType === 7) {
                    const view = new DataView(cellBuffer.buffer, cellBuffer.byteOffset + offset, 8);
                    const floatValue = view.getFloat64(0, false);
                    keyValue = floatValue.toString();
                } else {
                    // TEXT or BLOB
                    keyValue = new TextDecoder().decode(cellBuffer.slice(offset, offset + size));
                }
                
                // Compare key with search value
                // The left child contains all entries < keyValue
                // If searchValue <= keyValue, we need to check left child
                if (searchValue <= keyValue) {
                    await scanIndexRecursive(fileHandler, pageSize, leftChildPage, searchValue, rowids);
                }
                
                // If we've seen a key greater than searchValue, we can stop
                if (searchValue < keyValue) {
                    foundGreaterKey = true;
                    break;
                }
            }
        }
        
        // Only traverse rightmost child if we haven't found a greater key
        if (!foundGreaterKey) {
            const rightmostChild = await readRightmostPointer(fileHandler, pageOffset, isPage1);
            await scanIndexRecursive(fileHandler, pageSize, rightmostChild, searchValue, rowids);
        }
    }
}

// Fetch a specific row by rowid from a table
async function fetchRowByRowid(fileHandler: FileHandle, pageSize: number, rootPage: number, targetRowid: number): Promise<{ rowid: number, values: string[] } | null> {
    return await fetchRowByRowidRecursive(fileHandler, pageSize, rootPage, targetRowid);
}

// Recursive helper to find a row by rowid in the table B-tree
async function fetchRowByRowidRecursive(fileHandler: FileHandle, pageSize: number, pageNum: number, targetRowid: number): Promise<{ rowid: number, values: string[] } | null> {
    const pageOffset = (pageNum - 1) * pageSize;
    const isPage1 = pageNum === 1;
    
    const pageType = await readPageType(fileHandler, pageOffset, isPage1);
    
    if (pageType === 0x0d) {
        // Leaf page - search for the rowid
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, false);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = await readCell(fileHandler, pageOffset, cellOffset);
            const record = parseRecord(cellBuffer);
            
            if (record.rowid === targetRowid) {
                return record;
            }
        }
        
        return null;
    } else if (pageType === 0x05) {
        // Interior page - traverse to find the right child
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, true);
        
        // Check each cell to find the right branch
        for (const cellOffset of cellPointers) {
            const cellBuffer = new Uint8Array(16);
            await fileHandler.read(cellBuffer, 0, 16, pageOffset + cellOffset);
            
            const leftChildPage = new DataView(cellBuffer.buffer).getUint32(0, false);
            
            // Read the key (rowid) from this interior cell
            let offset = 4; // Skip left child pointer
            const [key, keyBytes] = readVarint(cellBuffer, offset);
            
            if (targetRowid <= key) {
                // Target is in the left subtree (including equal case)
                return await fetchRowByRowidRecursive(fileHandler, pageSize, leftChildPage, targetRowid);
            }
        }
        
        // If target is greater than all keys, check rightmost child
        const rightmostChild = await readRightmostPointer(fileHandler, pageOffset, isPage1);
        return await fetchRowByRowidRecursive(fileHandler, pageSize, rightmostChild, targetRowid);
    }
    
    return null;
}

// Parse CREATE TABLE to extract column names and identify INTEGER PRIMARY KEY
function parseCreateTable(sql: string): { columns: string[], integerPrimaryKeyColumn: string | null } {
    const columnsMatch = sql.match(/\(([^)]+)\)/s);
    if (!columnsMatch) {
        throw new Error("Failed to parse CREATE TABLE statement");
    }
    
    const columnsText = columnsMatch[1];
    const columnDefs = columnsText.split(',').map(col => col.trim());
    const columns: string[] = [];
    let integerPrimaryKeyColumn: string | null = null;
    
    for (const def of columnDefs) {
        const colName = def.split(/\s+/)[0].trim();
        columns.push(colName);
        
        // Check if this is an INTEGER PRIMARY KEY column
        const upperDef = def.toUpperCase();
        if (upperDef.includes('INTEGER') && upperDef.includes('PRIMARY') && upperDef.includes('KEY')) {
            integerPrimaryKeyColumn = colName;
        }
    }
    
    return { columns, integerPrimaryKeyColumn };
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
        const { values } = parseRecord(cellBuffer);
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
    
    // Read all rows from the table and count them
    const rows = await readTableCells(databaseFileHandler, pageSize, rootPage);
    
    console.log(rows.length);
    await databaseFileHandler.close();

} else if (command.toUpperCase().startsWith("SELECT") && !command.toUpperCase().includes("COUNT(*)")) {
    const parts = command.split(/\s+/);
    const selectIndex = parts.findIndex(p => p.toUpperCase() === "SELECT");
    const fromIndex = parts.findIndex(p => p.toUpperCase() === "FROM");
    const whereIndex = parts.findIndex(p => p.toUpperCase() === "WHERE");
    
    // Extract column names (may be comma-separated)
    const columnsStr = parts.slice(selectIndex + 1, fromIndex).join(' ');
    const columnNames = columnsStr.split(',').map(c => c.trim());
    const tableName = parts[fromIndex + 1];
    
    // Parse WHERE clause if present
    let whereColumn: string | null = null;
    let whereValue: string | null = null;
    if (whereIndex !== -1) {
        whereColumn = parts[whereIndex + 1];
        const valueStartIndex = whereIndex + 3; // WHERE column = value
        const rawValue = parts.slice(valueStartIndex).join(' ');
        whereValue = rawValue.replace(/^['"]|['"]$/g, ''); // Remove surrounding quotes
    }
    
    const databaseFileHandler = await open(databaseFilePath, constants.O_RDONLY);
    
    // Read page size
    const headerBuffer = new Uint8Array(100);
    await databaseFileHandler.read(headerBuffer, 0, 100, 0);
    const pageSize = new DataView(headerBuffer.buffer).getUint16(16);
    
    // Find table and get CREATE TABLE SQL
    const { rootPage, sql } = await findTable(databaseFileHandler, tableName, true);
    
    // Parse CREATE TABLE to find column indices and INTEGER PRIMARY KEY
    const { columns, integerPrimaryKeyColumn } = parseCreateTable(sql!);
    const columnIndices = columnNames.map(name => {
        const index = columns.indexOf(name);
        if (index === -1) {
            throw new Error(`Column ${name} not found in table ${tableName}`);
        }
        return index;
    });
    
    // Check which columns are INTEGER PRIMARY KEY (use rowid instead of values array)
    const useRowidForColumn = columnNames.map(name => name === integerPrimaryKeyColumn);
    
    // Find WHERE column index if present
    let whereColumnIndex = -1;
    let whereUsesRowid = false;
    if (whereColumn) {
        whereColumnIndex = columns.indexOf(whereColumn);
        if (whereColumnIndex === -1) {
            throw new Error(`Column ${whereColumn} not found in table ${tableName}`);
        }
        whereUsesRowid = whereColumn === integerPrimaryKeyColumn;
    }
    
    // Try to use index scan if WHERE clause matches an indexed column
    let rows: Array<{ rowid: number, values: string[] }> = [];
    let usedIndex = false;
    
    if (whereColumn === 'country' && whereValue) {
        // Try to find and use the index on country column
        try {
            const indexName = `idx_${tableName}_${whereColumn}`;
            const indexRootPage = await findIndex(databaseFileHandler, indexName);
            
            // Scan the index for matching rowids
            const matchingRowids = await scanIndex(databaseFileHandler, pageSize, indexRootPage, whereValue);
            
            // Fetch each matching row directly by rowid (much faster than table scan)
            for (const rowid of matchingRowids) {
                const row = await fetchRowByRowid(databaseFileHandler, pageSize, rootPage, rowid);
                if (row) {
                    rows.push(row);
                }
            }
            usedIndex = true;
        } catch (e) {
            // Index not found, fall back to full table scan
            rows = await readTableCells(databaseFileHandler, pageSize, rootPage);
        }
    } else {
        // No index available, do full table scan
        rows = await readTableCells(databaseFileHandler, pageSize, rootPage);
    }
    
    // Filter and print the requested columns
    rows.forEach(row => {
        // Apply WHERE filter if present and we didn't use index scan
        if (whereColumn && whereValue && !usedIndex) {
            const actualValue = whereUsesRowid ? row.rowid.toString() : row.values[whereColumnIndex];
            if (actualValue !== whereValue) {
                return;
            }
        }
        
        const values = columnIndices.map((idx, i) => {
            // If this column is INTEGER PRIMARY KEY, use rowid instead of values array
            if (useRowidForColumn[i]) {
                return row.rowid.toString();
            }
            return row.values[idx];
        });
        console.log(values.join('|'));
    });
    
    await databaseFileHandler.close();

} else {
    throw new Error(`Unknown command ${command}`);
}
