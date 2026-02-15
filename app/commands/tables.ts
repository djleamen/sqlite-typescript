import type { FileHandle } from 'fs/promises';
import { parseRecord } from '../parser/record.js';
import { readPageHeader, readCellPointers, readCell } from '../database/page.js';

/**
 * Print all table names in the database.
 * @param fileHandler - Open file handle to the SQLite database
 */
export async function handleTables(fileHandler: FileHandle): Promise<void> {
    const cellCount = await readPageHeader(fileHandler, 0, true);
    const cellPointers = await readCellPointers(fileHandler, 0, true, cellCount);
    
    const tableNames: string[] = [];
    for (const cellOffset of cellPointers) {
        const cellBuffer = await readCell(fileHandler, 0, cellOffset);
        const { values } = parseRecord(cellBuffer);
        tableNames.push(values[2]); // tbl_name is column 2
    }
    
    console.log(tableNames.join(' '));
}
