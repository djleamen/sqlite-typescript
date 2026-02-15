/**
 * Get the size in bytes of a value based on its SQLite serial type.
 * @param serialType - SQLite serial type code
 * @returns Size in bytes (or 0 for variable-length types)
 */
export function getSerialTypeSize(serialType: number): number {
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

/**
 * Parse a single value from a buffer based on its SQLite serial type.
 * @param buffer - The buffer containing the value
 * @param offset - Byte offset to the value
 * @param serialType - SQLite serial type code
 * @returns The parsed value as a string
 */
export function parseSerialValue(buffer: Uint8Array, offset: number, serialType: number): string {
    const size = getSerialTypeSize(serialType);
    
    if (serialType === 0) {
        return '';
    } else if (serialType === 8) {
        return '0';
    } else if (serialType === 9) {
        return '1';
    } else if (serialType >= 1 && serialType <= 6) {
        const view = new DataView(buffer.buffer, buffer.byteOffset + offset, size);
        let intValue = 0;
        
        if (size === 1) {
            intValue = view.getInt8(0);
        } else if (size === 2) {
            intValue = view.getInt16(0, false);
        } else if (size === 3) {
            const byte0 = buffer[offset];
            const byte1 = buffer[offset + 1];
            const byte2 = buffer[offset + 2];
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
        const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 8);
        const floatValue = view.getFloat64(0, false);
        return floatValue.toString();
    } else {
        const value = new TextDecoder().decode(buffer.slice(offset, offset + size));
        return value;
    }
}
