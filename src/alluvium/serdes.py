from json import JSONDecodeError, JSONDecoder, loads


def decode_non_delimited_json(content_unicode_str):
    """
    If you have a non delimited text file with valid json. You can use this
    to read everything.
    """
    decoder = JSONDecoder()
    decode_index, content_length = 0, len(content_unicode_str)

    objects = []
    while decode_index < content_length:
        try:
            obj, decode_index = decoder.raw_decode(content_unicode_str, decode_index)
            objects.append(obj)
        except JSONDecodeError:
            # Scan forward and keep trying to decode
            decode_index += 1
    return objects


def decode_new_line_delimited_json(content_fp):
    return [loads(line) for line in content_fp]
