import type { FileHandle } from 'fs/promises';
import { findTable, findIndex } from '../database/schema.js';
import { readTableCells } from '../database/table.js';
import { scanIndex } from '../database/index.js';
import { fetchRowByRowid } from '../database/table.js';
import { parseCreateTable } from '../parser/schema.js';

/**
 * Count rows in a table and print the result.
 * @param fileHandler - Open file handle to the SQLite database
 * @param pageSize - Size of database pages in bytes
 * @param tableName - Name of the table to count
 */
export async function handleSelectCount(fileHandler: FileHandle, pageSize: number, tableName: string): Promise<void> {
    const { rootPage } = await findTable(fileHandler, tableName);
    const rows = await readTableCells(fileHandler, pageSize, rootPage);
    
    console.log(rows.length);
}

/**
 * Execute a SELECT query and print matching rows.
 * Supports basic SELECT, FROM, and WHERE clauses. Can utilize indexes for optimized filtering.
 * @param fileHandler - Open file handle to the SQLite database
 * @param pageSize - Size of database pages in bytes
 * @param command - Full SQL SELECT command to execute
 */
export async function handleSelect(fileHandler: FileHandle, pageSize: number, command: string): Promise<void> {
    const parts = command.split(/\s+/);
    const selectIndex = parts.findIndex(p => p.toUpperCase() === "SELECT");
    const fromIndex = parts.findIndex(p => p.toUpperCase() === "FROM");
    const whereIndex = parts.findIndex(p => p.toUpperCase() === "WHERE");
    
    const columnsStr = parts.slice(selectIndex + 1, fromIndex).join(' ');
    const columnNames = columnsStr.split(',').map(c => c.trim());
    const tableName = parts[fromIndex + 1];
    
    let whereColumn: string | null = null;
    let whereValue: string | null = null;
    if (whereIndex !== -1) {
        whereColumn = parts[whereIndex + 1];
        const valueStartIndex = whereIndex + 3;
        const rawValue = parts.slice(valueStartIndex).join(' ');
        whereValue = rawValue.replace(/^['"]|['"]$/g, '');
    }
    
    const { rootPage, sql } = await findTable(fileHandler, tableName, true);
    const { columns, integerPrimaryKeyColumn } = parseCreateTable(sql!);
    const columnIndices = columnNames.map(name => {
        const index = columns.indexOf(name);
        if (index === -1) {
            throw new Error(`Column ${name} not found in table ${tableName}`);
        }
        return index;
    });
    
    const useRowidForColumn = columnNames.map(name => name === integerPrimaryKeyColumn);
    
    let whereColumnIndex = -1;
    let whereUsesRowid = false;
    if (whereColumn) {
        whereColumnIndex = columns.indexOf(whereColumn);
        if (whereColumnIndex === -1) {
            throw new Error(`Column ${whereColumn} not found in table ${tableName}`);
        }
        whereUsesRowid = whereColumn === integerPrimaryKeyColumn;
    }
    
    let rows: Array<{ rowid: number, values: string[] }> = [];
    let usedIndex = false;
    
    if (whereColumn === 'country' && whereValue) {
        try {
            const indexName = `idx_${tableName}_${whereColumn}`;
            const indexRootPage = await findIndex(fileHandler, indexName);
            
            // Scan the index for matching rowids
            const matchingRowids = await scanIndex(fileHandler, pageSize, indexRootPage, whereValue);
            
            // Fetch each matching row directly by rowid (much faster than table scan)
            for (const rowid of matchingRowids) {
                const row = await fetchRowByRowid(fileHandler, pageSize, rootPage, rowid);
                if (row) {
                    rows.push(row);
                }
            }
            usedIndex = true;
        } catch (e) {
            // Index not found, fall back to full table scan
            rows = await readTableCells(fileHandler, pageSize, rootPage);
        }
    } else {
        // No index available, do full table scan
        rows = await readTableCells(fileHandler, pageSize, rootPage);
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
}
