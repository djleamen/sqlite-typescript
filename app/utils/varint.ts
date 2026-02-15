/**
 * Read a variable-length integer (varint) from a buffer.
 * @param buffer - The buffer containing the varint
 * @param offset - Byte offset to start reading from
 * @returns Tuple of [value, bytesRead]
 */
export function readVarint(buffer: Uint8Array, offset: number): [number, number] {
    let value = 0;
    let bytesRead = 0;
    
    for (let i = 0; i < 9; i++) {
        const byte = buffer[offset + i];
        bytesRead++;
        
        if (i === 8) {
            // 9th byte uses all 8 bits
            value = (value << 8) | byte;
            break;
        } else {
            value = (value << 7) | (byte & 0x7f);
            if ((byte & 0x80) === 0) {
                break;
            }
        }
    }
    
    return [value, bytesRead];
}
