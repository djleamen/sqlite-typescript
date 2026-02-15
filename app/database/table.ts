import type { FileHandle } from 'fs/promises';
import { parseRecord } from '../parser/record.js';
import { readVarint } from '../utils/varint.js';
import { readPageType, readPageHeader, readCellPointers, readCell, readRightmostPointer } from './page.js';

/**
 * Read all rows from a table B-tree starting at the root page.
 * @param fileHandler - Open file handle to the database
 * @param pageSize - Size of database pages in bytes
 * @param rootPage - Root page number of the table B-tree
 * @returns Array of all rows with their rowids and column values
 */
export async function readTableCells(fileHandler: FileHandle, pageSize: number, rootPage: number): Promise<Array<{ rowid: number, values: string[] }>> {
    const rows: Array<{ rowid: number, values: string[] }> = [];
    await readTableCellsRecursive(fileHandler, pageSize, rootPage, rows);
    return rows;
}

/**
 * Read rows from a table B-tree, returning only those with rowids in the provided set.
 * @param fileHandler - Open file handle to the database
 * @param pageSize - Size of database pages in bytes
 * @param rootPage - Root page number of the table B-tree
 * @param rowidSet - Set of rowids to include
 * @param rows - Output array to accumulate matching rows
 */
export async function readTableCellsFiltered(fileHandler: FileHandle, pageSize: number, rootPage: number, rowidSet: Set<number>, rows: Array<{ rowid: number, values: string[] }>): Promise<void> {
    await readTableCellsFilteredRecursive(fileHandler, pageSize, rootPage, rowidSet, rows);
}

/**
 * Fetch a single row from a table B-tree by its rowid.
 * @param fileHandler - Open file handle to the database
 * @param pageSize - Size of database pages in bytes
 * @param rootPage - Root page number of the table B-tree
 * @param targetRowid - The rowid to search for
 * @returns The row if found, null otherwise
 */
export async function fetchRowByRowid(fileHandler: FileHandle, pageSize: number, rootPage: number, targetRowid: number): Promise<{ rowid: number, values: string[] } | null> {
    return await fetchRowByRowidRecursive(fileHandler, pageSize, rootPage, targetRowid);
}

/**
 * Recursively traverse a table B-tree to collect rows matching a rowid set.
 * @param fileHandler - Open file handle to the database
 * @param pageSize - Size of database pages in bytes
 * @param pageNum - Current page number in the B-tree
 * @param rowidSet - Set of rowids to include
 * @param rows - Output array to accumulate matching rows
 */
async function readTableCellsFilteredRecursive(fileHandler: FileHandle, pageSize: number, pageNum: number, rowidSet: Set<number>, rows: Array<{ rowid: number, values: string[] }>): Promise<void> {
    const pageOffset = (pageNum - 1) * pageSize;
    const isPage1 = pageNum === 1;
    
    const pageType = await readPageType(fileHandler, pageOffset, isPage1);
    
    if (pageType === 0x0d) {
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

/**
 * Recursively traverse a table B-tree to collect all rows.
 * @param fileHandler - Open file handle to the database
 * @param pageSize - Size of database pages in bytes
 * @param pageNum - Current page number in the B-tree
 * @param rows - Output array to accumulate all rows
 */
async function readTableCellsRecursive(fileHandler: FileHandle, pageSize: number, pageNum: number, rows: Array<{ rowid: number, values: string[] }>): Promise<void> {
    const pageOffset = (pageNum - 1) * pageSize;
    const isPage1 = pageNum === 1;
    
    const pageType = await readPageType(fileHandler, pageOffset, isPage1);
    
    if (pageType === 0x0d) {
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, false);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = await readCell(fileHandler, pageOffset, cellOffset);
            const record = parseRecord(cellBuffer);
            rows.push(record);
        }
    } else if (pageType === 0x05) {
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, true);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = new Uint8Array(16);
            await fileHandler.read(cellBuffer, 0, 16, pageOffset + cellOffset);
            
            const leftChildPage = new DataView(cellBuffer.buffer).getUint32(0, false);
            await readTableCellsRecursive(fileHandler, pageSize, leftChildPage, rows);
        }
        
        const rightmostChild = await readRightmostPointer(fileHandler, pageOffset, isPage1);
        await readTableCellsRecursive(fileHandler, pageSize, rightmostChild, rows);
    }
}

/**
 * Recursively search a table B-tree for a specific rowid.
 * @param fileHandler - Open file handle to the database
 * @param pageSize - Size of database pages in bytes
 * @param pageNum - Current page number in the B-tree
 * @param targetRowid - The rowid to search for
 * @returns The row if found, null otherwise
 */
async function fetchRowByRowidRecursive(fileHandler: FileHandle, pageSize: number, pageNum: number, targetRowid: number): Promise<{ rowid: number, values: string[] } | null> {
    const pageOffset = (pageNum - 1) * pageSize;
    const isPage1 = pageNum === 1;
    
    const pageType = await readPageType(fileHandler, pageOffset, isPage1);
    
    if (pageType === 0x0d) {
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
        const cellCount = await readPageHeader(fileHandler, pageOffset, isPage1);
        const cellPointers = await readCellPointers(fileHandler, pageOffset, isPage1, cellCount, true);
        
        for (const cellOffset of cellPointers) {
            const cellBuffer = new Uint8Array(16);
            await fileHandler.read(cellBuffer, 0, 16, pageOffset + cellOffset);
            
            const leftChildPage = new DataView(cellBuffer.buffer).getUint32(0, false);
            
            let offset = 4;
            const [key, keyBytes] = readVarint(cellBuffer, offset);
            
            if (targetRowid <= key) {
                return await fetchRowByRowidRecursive(fileHandler, pageSize, leftChildPage, targetRowid);
            }
        }
        
        const rightmostChild = await readRightmostPointer(fileHandler, pageOffset, isPage1);
        return await fetchRowByRowidRecursive(fileHandler, pageSize, rightmostChild, targetRowid);
    }
    
    return null;
}
