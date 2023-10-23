import json
import msgpack
import bz2


def iterate_bytes_jsons(fin, batch_size=1000):
    current = []
    for l in fin:
        if l.startswith(b'{'):
            current.append(l)
        if len(current) >= batch_size:
            yield from json.loads(
                '[' + b"".join(current).decode('utf-8').rstrip(',\n') + ']'
            )
            current = []
    if len(current) > 0:
        yield from json.loads(
            '[' + b"".join(current).decode('utf-8').rstrip(',\n') + ']'
        )
        current = []


def iterate_text_jsons(fin, batch_size=1000):
    current = []
    for l in fin:
        if l.startswith('{'):
            current.append(l)
        if len(current) >= batch_size:
            yield from json.loads('[' + "".join(current).rstrip(',\n') + ']')
            current = []
    if len(current) > 0:
        yield from json.loads('[' + "".join(current).rstrip(',\n') + ']')
        current = []


def iterate_message_packs(fin):

    yield from msgpack.Unpacker(fin, encoding='utf-8', use_list=False)


def open_wikidata_file(path, batch_size):
    if path.endswith('bz2'):
        with bz2.open(path, 'rb') as fin:
            yield from iterate_bytes_jsons(fin, batch_size)
    elif path.endswith('json'):
        with open(path, 'rt') as fin:
            yield from iterate_text_jsons(fin, batch_size)
    elif path.endswith('mp'):
        with open(path, 'rb') as fin:
            yield from iterate_message_packs(fin)
    else:
        raise ValueError(
            "unknown extension for wikidata. "
            "Expecting bz2, json, or mp (msgpack)."
        )
