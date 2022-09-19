import glom

def dict_generator(indict, pre=None):
    """Processes python dictionary and return list of key values
    :param indict
    :param pre
    :return generator"""
    pre = pre[:] if pre else []
    if isinstance(indict, dict):
        for key, value in list(indict.items()):
            if key == "_id":
                continue
            if isinstance(value, dict):
                #                print 'dgen', value, key, pre
                for d in dict_generator(value, pre + [key]):
                    yield d
            elif isinstance(value, list) or isinstance(value, tuple):
                for v in value:
                    if isinstance(v, dict):
                        #                print 'dgen', value, key, pre
                        for d in dict_generator(v, pre + [key]):
                            yield d
#                    for d in dict_generator(v, [key] + pre):
#                        yield d
            else:
                yield pre + [key, value]
    else:
        yield indict


def get_dict_keys(iterable, limit=1000):
    n = 0
    keys = []
    for item in iterable:
        if limit and n > limit:
            break
        n += 1
        dk = dict_generator(item)
        for i in dk:
            k = ".".join(i[:-1])
            if k not in keys:
                keys.append(k)
    return keys


def join_fields(record, fields, destination, sep='', delete_joined=False):
    values = []
    for field in fields:
        try:
            values.append(str(glom.glom(record, field)))
        except glom.core.PathAccessError:
            values.append('')
    result = sep.join(values)
    if delete_joined:
        for field in fields:
            try:
                glom.delete(record, field)
            except glom.mutation.PathDeleteError:
                pass
    record[destination] = result
    return record



def split_field(record, field, prefix=None, sep=',', limit=None, delete_original=False):
    """Split field of the record"""
    prefix = field if not prefix else prefix
    try:
        value = glom.glom(record, field)
    except glom.core.PathAccessError:
        return record
    if len(sep) > 0:    
        items = value.split(sep) if limit is None else value.split(sep)
        if len(items) > 0:
            n = 0
            for item in items:
                n += 1
                path = prefix + '_%d' % (n)
                glom.assign(record, path, item)
    if delete_original:
        glom.delete(record, field)            
    return record


if __name__ == "__main__":
    data = {'one': 'two', 'three': {'four': 'five', 'six': {'seven': 'eight'}}}
    print(flatten_data(data))