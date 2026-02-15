import type { FileHandle } from 'fs/promises';

/**
 * Read the cell count from a page header.
 * @param fileHandler - Open file handle to the database
 * @param pageOffset - Byte offset of the page in the file
 * @param isPage1 - Whether this is page 1 (has 100-byte header offset)
 * @returns The number of cells on the page
 */
export async function readPageHeader(fileHandler: FileHandle, pageOffset: number, isPage1: boolean): Promise<number> {
    const headerOffset = isPage1 ? 100 : 0;
    const buffer = new Uint8Array(12);
    await fileHandler.read(buffer, 0, 12, pageOffset + headerOffset);
    return new DataView(buffer.buffer).getUint16(3);
}

/**
 * Read the type byte from a page header.
 * @param fileHandler - Open file handle to the database
 * @param pageOffset - Byte offset of the page in the file
 * @param isPage1 - Whether this is page 1 (has 100-byte header offset)
 * @returns The page type (0x0d = table leaf, 0x05 = table interior, 0x0a = index leaf, 0x02 = index interior)
 */
export async function readPageType(fileHandler: FileHandle, pageOffset: number, isPage1: boolean): Promise<number> {
    const headerOffset = isPage1 ? 100 : 0;
    const buffer = new Uint8Array(1);
    await fileHandler.read(buffer, 0, 1, pageOffset + headerOffset);
    return buffer[0];
}

/**
 * Read the rightmost child page pointer from an interior page header.
 * @param fileHandler - Open file handle to the database
 * @param pageOffset - Byte offset of the page in the file
 * @param isPage1 - Whether this is page 1 (has 100-byte header offset)
 * @returns The page number of the rightmost child
 */
export async function readRightmostPointer(fileHandler: FileHandle, pageOffset: number, isPage1: boolean): Promise<number> {
    const headerOffset = isPage1 ? 100 : 0;
    const buffer = new Uint8Array(12);
    await fileHandler.read(buffer, 0, 12, pageOffset + headerOffset);
    return new DataView(buffer.buffer).getUint32(8);
}

/**
 * Read the cell pointer array from a page header.
 * @param fileHandler - Open file handle to the database
 * @param pageOffset - Byte offset of the page in the file
 * @param isPage1 - Whether this is page 1 (has 100-byte header offset)
 * @param cellCount - Number of cells on the page
 * @param isInterior - Whether this is an interior page (affects pointer array offset)
 * @returns Array of byte offsets for each cell on the page
 */
export async function readCellPointers(fileHandler: FileHandle, pageOffset: number, isPage1: boolean, cellCount: number, isInterior: boolean = false): Promise<number[]> {
    const headerOffset = isPage1 ? 100 : 0;
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

/**
 * Read a cell from a page into a buffer.
 * @param fileHandler - Open file handle to the database
 * @param pageOffset - Byte offset of the page in the file
 * @param cellOffset - Byte offset of the cell within the page
 * @returns Buffer containing the cell data
 */
export async function readCell(fileHandler: FileHandle, pageOffset: number, cellOffset: number): Promise<Uint8Array> {
    const buffer = new Uint8Array(2000);
    await fileHandler.read(buffer, 0, buffer.length, pageOffset + cellOffset);
    return buffer;
}
