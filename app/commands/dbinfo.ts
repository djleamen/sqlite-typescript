import type { FileHandle } from 'fs/promises';
import { readPageHeader } from '../database/page.js';

/**
 * Print SQLite database metadata including page size and table count.
 * @param fileHandler - Open file handle to the SQLite database
 */
export async function handleDbInfo(fileHandler: FileHandle): Promise<void> {
    const buffer: Uint8Array = new Uint8Array(100);
    await fileHandler.read(buffer, 0, buffer.length, 0);

    const pageSize = new DataView(buffer.buffer, 0, buffer.byteLength).getUint16(16);
    console.log(`database page size: ${pageSize}`);

    const numberOfTables = await readPageHeader(fileHandler, 0, true);
    console.log(`number of tables: ${numberOfTables}`);
}
