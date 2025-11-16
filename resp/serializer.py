def serialize(characters, is_error=False):
    if isinstance(characters, list):
        serialized_array = serialize_array(characters)
        return serialized_array
    encoded = str(characters).encode('utf-8')
    if is_error:
        return b"-" + encoded + b"\r\n" 
    # Check if the characters passed is a string
    if(isinstance(characters, str)):
        # Use the number of bytes to differentiate between bulky and simple strings
        bytes_no = len(encoded)
        if (bytes_no > 64):
            return f"${bytes_no}\r\n".encode('utf-8') + encoded + b"\r\n" 
        else:
            return b"+" + encoded + b"\r\n" 
    if (isinstance(characters, int)):
        return b":" + encoded + b"\r\n"
    
def serialize_array(array):
    serialized = b"*" + str(len(array)).encode('utf-8') + b"\r\n"
    for element in array:
        serialized_element = serialize(element)
        serialized = serialized + serialized_element
    return serialized


def deserialize(resp_bytes):
    """
    Deserialize RESP-encoded bytes into Python objects:
    - Integers → int
    - Simple strings / bulk strings → str
    - Errors → str (could raise exception if you want)
    - Arrays → list (nested arrays supported)
    """
    def parse(idx):
        if idx >= len(resp_bytes):
            raise ValueError("Unexpected end of input")

        prefix = resp_bytes[idx:idx+1]
        idx += 1

        if prefix == b'+':  
            end = resp_bytes.find(b'\r\n', idx)
            val = resp_bytes[idx:end].decode('utf-8')
            idx = end + 2
            return val, idx

        elif prefix == b'-': 
            end = resp_bytes.find(b'\r\n', idx)
            val = resp_bytes[idx:end].decode('utf-8')
            idx = end + 2
            return val, idx

        elif prefix == b':': 
            end = resp_bytes.find(b'\r\n', idx)
            val = int(resp_bytes[idx:end])
            idx = end + 2
            return val, idx

        elif prefix == b'$':  
            end = resp_bytes.find(b'\r\n', idx)
            length = int(resp_bytes[idx:end])
            idx = end + 2
            if length == -1:
                return None, idx  
            val = resp_bytes[idx:idx+length].decode('utf-8')
            idx += length + 2  
            return val, idx

        elif prefix == b'*':  
            end = resp_bytes.find(b'\r\n', idx)
            count = int(resp_bytes[idx:end])
            idx = end + 2
            arr = []
            for _ in range(count):
                elem, idx = parse(idx)
                arr.append(elem)
            return arr, idx

        else:
            raise ValueError(f"Unknown RESP type: {prefix}")

    result, _ = parse(0)
    return result


