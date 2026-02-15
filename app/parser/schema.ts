/**
 * Parse a CREATE TABLE statement to extract column names and identify the INTEGER PRIMARY KEY.
 * @param sql - The CREATE TABLE SQL statement
 * @returns Columns array and the name of the INTEGER PRIMARY KEY column (if present)
 */
export function parseCreateTable(sql: string): { columns: string[], integerPrimaryKeyColumn: string | null } {
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
        
        const upperDef = def.toUpperCase();
        if (upperDef.includes('INTEGER') && upperDef.includes('PRIMARY') && upperDef.includes('KEY')) {
            integerPrimaryKeyColumn = colName;
        }
    }
    
    return { columns, integerPrimaryKeyColumn };
}
