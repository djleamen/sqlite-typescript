import type { FileHandle } from 'fs/promises';
import { readVarint } from '../utils/varint.js';
import { getSerialTypeSize } from '../utils/serialTypes.js';
import { readPageType, readPageHeader, readCellPointers, readCell, readRightmostPointer } from './page.js';

/**
 * Scan an index tree and collect all rowids where the indexed column matches the search value.
 * @param fileHandler - Open file handle to the database
 * @param pageSize - Size of database pages in bytes
 * @param rootPage - Root page number of the index
 * @param searchValue - Value to search for in the indexed column
 * @returns Array of rowids matching the search value
 */
export async function scanIndex(fileHandler: FileHandle, pageSize: number, rootPage: number, searchValue: string): Promise<number[]> {
    const rowids: number[] = [];
    await scanIndexRecursive(fileHandler, pageSize, rootPage, searchValue, rowids);
    return rowids;
}

/**
 * Recursively traverse an index B-tree to find all matching entries.
 * @param fileHandler - Open file handle to the database
 * @param pageSize - Size of database pages in bytes
 * @param pageNum - Current page number in the B-tree
 * @param searchValue - Value to search for in the indexed column
 * @param rowids - Accumulated array of matching rowids
 */
async function scanIndexRecursive(fileHandler: FileHandle, pageSize: number, pageNum: number, searchValue: string, rowids: number[]): Promise<void> {
    const pageOffset = (pageNum - 1) * pageSize;
    const isPage1 = pageNum === 1;
    
    const pageType = await readPageType(fileHandler, pageOffset, isPage1);
    
    if (pageType === 0x0a) {
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, false);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = await readCell(fileHandler, pageOffset, cellOffset);
            const { indexedValue, rowid } = parseIndexEntry(cellBuffer);
            
            if (indexedValue === searchValue && rowid !== null) {
                rowids.push(rowid);
            }
        }
    } else if (pageType === 0x02) {
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, true);
        
        let foundGreaterKey = false;
        
        // Read each cell and check the key to decide which children to traverse
        for (const cellOffset of cellPointers) {
            const cellBuffer = new Uint8Array(1000);
            await fileHandler.read(cellBuffer, 0, cellBuffer.length, pageOffset + cellOffset);
            
            const leftChildPage = new DataView(cellBuffer.buffer).getUint32(0, false);
            
            // Parse the key (first indexed value) from this interior cell
            const { indexedValue: keyValue } = parseIndexEntry(cellBuffer, 4); // Skip 4-byte left child pointer
            
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
        
        // Only traverse rightmost child if we haven't found a greater key
        if (!foundGreaterKey) {
            const rightmostChild = await readRightmostPointer(fileHandler, pageOffset, isPage1);
            await scanIndexRecursive(fileHandler, pageSize, rightmostChild, searchValue, rowids);
        }
    }
}

/**
 * Parse an index entry to extract the indexed column value and its rowid.
 * @param cellBuffer - Buffer containing the index cell data
 * @param startOffset - Byte offset to start parsing from
 * @returns The indexed column value and associated rowid
 */
function parseIndexEntry(cellBuffer: Uint8Array, startOffset: number = 0): { indexedValue: string, rowid: number | null } {
    let offset = startOffset;
    
    const [payloadSize, payloadSizeBytes] = readVarint(cellBuffer, offset);
    offset += payloadSizeBytes;
    
    const [headerSize, headerSizeBytes] = readVarint(cellBuffer, offset);
    offset += headerSizeBytes;
    
    const serialTypes: number[] = [];
    const headerStart = offset - headerSizeBytes;
    while (offset - headerStart < headerSize) {
        const [serialType, serialTypeBytes] = readVarint(cellBuffer, offset);
        serialTypes.push(serialType);
        offset += serialTypeBytes;
    }
    
    let indexedValue = '';
    if (serialTypes.length >= 1) {
        const firstSerialType = serialTypes[0];
        indexedValue = parseIndexValue(cellBuffer, offset, firstSerialType);
        offset += getSerialTypeSize(firstSerialType);
    }
    
    let rowid: number | null = null;
    if (serialTypes.length >= 2) {
        // Skip intermediate values
        for (let i = 1; i < serialTypes.length - 1; i++) {
            offset += getSerialTypeSize(serialTypes[i]);
        }
        
        // Read rowid (last value)
        const rowidSerialType = serialTypes[serialTypes.length - 1];
        const rowidSize = getSerialTypeSize(rowidSerialType);
        
        if (rowidSerialType >= 1 && rowidSerialType <= 6 && rowidSize > 0) {
            const view = new DataView(cellBuffer.buffer, cellBuffer.byteOffset + offset, rowidSize);
            
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
        }
    }
    
    return { indexedValue, rowid };
}

/**
 * Parse a single value from an index cell based on its serial type.
 * @param cellBuffer - Buffer containing the index cell data
 * @param offset - Byte offset to the value
 * @param serialType - SQLite serial type of the value
 * @returns The parsed value as a string
 */
function parseIndexValue(cellBuffer: Uint8Array, offset: number, serialType: number): string {
    const size = getSerialTypeSize(serialType);
    
    if (serialType === 0) {
        return '';
    } else if (serialType === 8) {
        return '0';
    } else if (serialType === 9) {
        return '1';
    } else if (serialType >= 1 && serialType <= 6) {
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
        
        return intValue.toString();
    } else if (serialType === 7) {
        const view = new DataView(cellBuffer.buffer, cellBuffer.byteOffset + offset, 8);
        const floatValue = view.getFloat64(0, false);
        return floatValue.toString();
    } else {
        // TEXT or BLOB
        return new TextDecoder().decode(cellBuffer.slice(offset, offset + size));
    }
}
