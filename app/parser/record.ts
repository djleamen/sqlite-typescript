import { readVarint } from '../utils/varint.js';
import { getSerialTypeSize, parseSerialValue } from '../utils/serialTypes.js';

/**
 * Parse a table record to extract rowid and column values.
 * @param buffer - Buffer containing the record data
 * @returns Object with rowid and array of column values
 */
export function parseRecord(buffer: Uint8Array): { rowid: number, values: string[] } {
    let offset = 0;
    
    const [recordSize, recordSizeBytes] = readVarint(buffer, offset);
    offset += recordSizeBytes;
    
    const [rowid, rowidBytes] = readVarint(buffer, offset);
    offset += rowidBytes;
    
    const [headerSize, headerSizeBytes] = readVarint(buffer, offset);
    offset += headerSizeBytes;
    
    const serialTypes: number[] = [];
    const headerStart = offset - headerSizeBytes;
    while (offset - headerStart < headerSize) {
        const [serialType, serialTypeBytes] = readVarint(buffer, offset);
        serialTypes.push(serialType);
        offset += serialTypeBytes;
    }
    
    const values: string[] = [];
    for (const serialType of serialTypes) {
        const value = parseSerialValue(buffer, offset, serialType);
        values.push(value);
        offset += getSerialTypeSize(serialType);
    }
    
    return { rowid, values };
}
