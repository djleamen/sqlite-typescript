import type { FileHandle } from 'fs/promises';
import { parseRecord } from '../parser/record.js';
import { readCell, readCellPointers } from './page.js';

/**
 * Find a table in the sqlite_schema and return its root page number.
 * @param fileHandler - Open file handle to the database
 * @param tableName - Name of the table to find
 * @param includeSql - If true, also return the CREATE TABLE SQL statement
 * @returns Root page number and optionally the CREATE TABLE SQL
 */
export async function findTable(fileHandler: FileHandle, tableName: string, includeSql: boolean = false): Promise<{ rootPage: number, sql?: string }> {
    const cellCount = await readPageHeader(fileHandler, 0, true);
    const cellPointers = await readCellPointers(fileHandler, 0, true, cellCount);
    
    for (const cellOffset of cellPointers) {
        const cellBuffer = await readCell(fileHandler, 0, cellOffset);
        const { values } = parseRecord(cellBuffer);
        
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

/**
 * Find an index in the sqlite_schema and return its root page number.
 * @param fileHandler - Open file handle to the database
 * @param indexName - Name of the index to find
 * @returns Root page number of the index
 */
export async function findIndex(fileHandler: FileHandle, indexName: string): Promise<number> {
    const cellCount = await readPageHeader(fileHandler, 0, true);
    const cellPointers = await readCellPointers(fileHandler, 0, true, cellCount);
    
    for (const cellOffset of cellPointers) {
        const cellBuffer = await readCell(fileHandler, 0, cellOffset);
        const { values } = parseRecord(cellBuffer);
        
        if (values[0] === 'index' && values[1] === indexName) {
            return parseInt(values[3]);
        }
    }
    
    throw new Error(`Index ${indexName} not found`);
}

// Import this at the end to avoid circular dependency
import { readPageHeader } from './page.js';
