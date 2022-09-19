# -*- coding: utf-8 -*-
import logging
import pymongo
import bson
import glom
from .operations import common, processing

MARK_STAR = 1
MARK_FLAG = 2
DEFAULT_CATEGORICAL_THRESHOLD = 10

class RowSet:
    """Set of rows in MongoDB for data manipulation"""
    def __init__(self, refiner, query):
        self.refiner = refiner
        self.query = query        

    def count(self):
        """Number of documents in Row set"""
        return self.refiner.coll.count_documents(query)

    def star(self):
        """Annotate rows in Row set"""
        self.refiner.coll.update_many(query, {'$set' : {'__mark' : MARK_STAR}})

    def flag(self):
        """Annotate rows in Row set"""
        self.refiner.coll.update_many(query, {'$set' : {'__mark' : MARK_FLAG}})

    def remove(self):
        """Remove all rows in Row set"""
        self.refiner.coll.remove(query)

    def transform(self, function):
        """Transforms each record of rowset"""
        self.refiner.transform_records(self.query, function)



class Column:
    """Column (e.g. field) of MongoDB database collection"""
    def __init__(self, refiner, name, colspec=None):
        self.refiner = refiner
        self.name = name
        self.__metadata = colspec
    
    def items(self, start=0, limit=10, query={}):
        """Return list of column items"""
        values = []
        query[self.name] = { '$exists' : True}
        for item in self.cursor(query).skip(start).limit(limit):
            values.append(glom.glom(item, self.name))            
        return values

    def __repr__(self):
        return 'Column %s (%s)' % (self.name, self.refiner.collname)

    def metadata(self):
        """Return column spec if exists"""
        return self.__metadata
    
    def cursor(self, query={}):
        """Returm cursor with this column only"""
        query[self.name] = {'$exists': True}
        return self.refiner.coll.find(query, {self.name: 1})

    def distinct(self, query={}):
        """Returns list of unique column values"""
        return self.refiner.distinct(self.name, query)

    def count(self, query={}):
        """Count all column values"""
        query[self.name] = {'$exists': True}
        return self.refiner.count(query)

    def transform(self, query, function, mode='full'):
        """Apply Python function to all values of query of selected column and replace it's value"""
        self.refiner.transform_column(query, function, column=self.name, mode=mode)

    def new(self, name, function=common.do_copy, query={}):
        """Creates new column from this using external function"""
        self.refiner.transform_column(query, function, column=self.name, new_column=name)
        return Column(self.refiner, name)

    def concat(self, prefix=None, postfix=None, destination=None, query={}):
        """Concatenate field value with prefix and/or postfix static strings"""
        options = [prefix, '$' + self.name] if prefix is not None else ['$' + self.name, ]
        if postfix != None:
            options.append(postfix)
        self.refiner.column_string_manipulation(self.name if destination is None else destination, command='$concat', options=options, query=query)

    def trim(self, query={}, chars=None, mode='quick', trim_type='both'):
        """Removes left or right or both sides characters from column strings"""
        if mode == 'quick':
            if trim_type == 'left':
                command = '$ltrim'
            elif trim_type == 'right':
                command = '$rtrim'
            else:
                command = '$trim'
            options = {'input' : '$' + self.name} if not chars else {'input' : '$' + self.name, 'chars' : chars}
            self.refiner.column_string_manipulation(self.name, command=command, options=options, query=query)
        else:        
            self.transform(query, common.do_trim, mode=mode)

    def upper(self, query={}, mode='quick'):
        """Upper string"""
        if mode == 'quick':
            self.refiner.column_string_manipulation(self.name, command='$toUpper', options="$" + self.name, query=query)
        else:        
            self.transform(query, common.do_upper, mode=mode)

    def float_(self, query={}, mode='quick'):
        """Convert field to float"""
        if mode == 'quick':
            self.refiner.column_string_manipulation(self.name, command='$toFloat', options="$" + self.name, query=query)
        else:
            self.transform(query, common.do_float, mode=mode)

    def todate(self, query={}, mode='quick'):
        """Convert field to date/datetime"""
        if mode == 'quick':
            self.refiner.column_string_manipulation(self.name, command='$toDate', options="$" + self.name, query=query)
        else:
            self.transform(query, common.do_simple_date, mode=mode)

    def number(self, query={}, mode='quick'):
        """Convert field to integer"""
        if mode == 'quick':
            self.refiner.column_string_manipulation(self.name, command='$toInt', options="$" + self.name, query=query)
        else:
            self.transform(query, common.do_int, mode=mode)

    def string(self, query={}, mode='quick'):
        """Convert field to string"""
        if mode == 'quick':
            self.refiner.column_string_manipulation(self.name, command='$toString', options="$" + self.name, query=query)
        else:
            self.transform(query, common.do_string, mode=mode)

    def lower(self, query={}, mode='quick'):
        """Lower string"""        
        if mode == 'quick':
            self.refiner.column_string_manipulation(self.name, command='$toLower', options="$" + self.name, query=query)
        else:
            self.transform(query, common.do_lower, mode=mode)

    def title(self, query={}):
        """Title string (note: there is no quick function, only slow"""
        self.transform(query, common.do_title)

    def rename(self, new_name, query={}):
        """Rename column"""
        self.refiner.rename_column(self.name, new_name)
        self.name = new_name

    def replace(self, old_value, new_value):
        """Replaces substring in column values"""
        self.refiner.replace_substring(self.name, old_value, new_value)

    def drop(self, query={}):
        """Remove the column"""
        self.refiner.remove_column(self.name, query)

    def scan(self, limit=None):
        """Scan column and save stats in metadata"""
        metadata = {}
        n = 0
        datatypes = {}
        values = {}
        total = self.count()
        for item in self.cursor():
            n += 1
            value = glom.glom(item, self.name)        
            _dtype = value.__class__.__qualname__
            v = datatypes.get(_dtype, 0)
            datatypes[_dtype] = v + 1

            v = values.get(value, 0)
            values[value] = v + 1
        uniq_val = len(values.keys())
        share = 100.0 * uniq_val / total
        sorted_datatypes = sorted(datatypes.items(), key=lambda item: item[1])
        metadata = {'datatype' : sorted_datatypes[0][0], 'categorical' : share < self.refiner.categorical_threshold, 'unique_count' : uniq_val, 'unique_ratio' : share, 'row_count' : total}
        if self.__metadata:
            self.__metadata.update(metadata)
        else:
            self.__metadata = metadata
        pass

class Refiner:
    def __init__(self, dbname, collname, hostname='localhost', port=27017, username=None, password=None, categorical_threshold=DEFAULT_CATEGORICAL_THRESHOLD):
        """Init refiner class with dbname, collname and e.t.c."""
        self.dbname = dbname
        self.collname = collname
        self.client = pymongo.MongoClient(hostname, port)
        self.db = self.client[dbname]
        self.coll = self.db[collname]
        self.categorical_threshold = categorical_threshold
        self.__metadata = None
        self.default_rowset = RowSet(self, {})
        pass

    def metadata(self):
        """Return column spec if exists"""
        return self.__metadata

    def scan(self, limit=None, calc_freqtables=False):
        """Scan dataset"""
        logging.debug('Start: collection scan %s' % (self.collname))

        total  = self.count()
        columns = []
        n = 0
        cols_datatypes = {}
        cols_values = {}
        cols_total = {}
        for row in self.cursor():
            n += 1
            dk = processing.dict_generator(row)
            for i in dk:
                key = '.'.join(i[:-1])
                value = i[-1]
                if key not in columns:
                    columns.append(key)
                if key not in cols_total:
                    cols_total[key] = 0
                cols_total[key] += 1
                if key not in cols_datatypes.keys():
                    cols_datatypes[key] = {}
                _dtype = value.__class__.__qualname__
                v = cols_datatypes[key].get(_dtype, 0)
                cols_datatypes[key][_dtype] = v + 1

                if key not in cols_values.keys():
                    cols_values[key] = {}

                v = cols_values[key].get(value, 0)
                cols_values[key][value] = v + 1

            if limit and n > limit:
                break
        colspecs = {}
        freqtables = {}
        for k in cols_datatypes.keys():
            sorted_datatypes = sorted(cols_datatypes[k].items(), key=lambda item: item[1], reverse=True)
            datatype = sorted_datatypes[1][0] if sorted_datatypes[0][0] == 'NoneType' and len(sorted_datatypes) > 1 else sorted_datatypes[0][0]
            share = 100.0*(len(cols_values[k])) / cols_total[k]
            categorical = share < self.categorical_threshold if datatype == 'str' else False
            colspecs[k] = {'datatype': datatype, 'categorical' : categorical, 'unique_count' : len(cols_values[k]), 'unique_ratio' : share, 'row_count' : cols_total[k]}
            if categorical and datatype == 'str' and calc_freqtables:
                freqtables[k] = sorted(cols_values[k].items(), key=lambda item: item[1], reverse=True)
        metadata = {'row_count' : total, 'columns' : columns, 'colspecs' : colspecs}
        if calc_freqtables:
            metadata['freqtables'] = freqtables
        self.__metadata = metadata
        logging.debug('End: collection scan %s' % (self.collname))

    def cursor(self, query={}):
        """Returm cursor with all data"""        
        return self.coll.find(query)

    def get_rowset(self, query={}):
        """Return row set as an object"""
        return RowSet(self, query)

    def get_column(self, name):
        """Return column as an object"""
        if self.__metadata and 'colspecs' in self.__metadata and  name in self.__metadata['colspecs'].keys():
            colspec = self.__metadata['colspecs'][name]
        else:
            colspec = None
        return Column(self, name, colspec=colspec)
    
    def columns(self):
        """Return list of columns"""
        result = []
        for name in self.__metadata['colspecs'].keys():
            result.append(self.get_column(name))
        return result

    def count(self, query={}):
        """Count all column values"""
        return self.coll.count_documents(query)

    def distinct(self, column_name, query={}):
        """Returns list of unique column/field values"""
        return self.coll.distinct(column_name, query)

    def replace_substring(self, column, old_value, new_value, query={}, mode='one'):        
        """Replace one or all matching substring in a string column/field"""
        command = '$replaceAll' if mode == 'all' else '$replaceOne'
        query[column] = {'$regex': '%s' % (old_value)}
        update = [{'$set' : { column : {command: {'input' : '$' + column, 'find' : old_value, 'replacement' : new_value}}}}]
        logging.debug('Applying operation for query %s with %s' % (str(query), str(update)))
        self.coll.update_many(query, update)

    def column_string_manipulation(self, column, command, options={}, query={}):
        """Apply one of string manipulation functions (trim, upper, lower and e.t.c.) to column"""
        update = [{'$set' : { column : {command: options}}}]
        logging.debug('Applying operation for query %s with %s' % (str(query), str(update)))
        self.coll.update_many(query, update)

    def remove_column(self, column, query={}):
        """Remove column/field from collection"""
        logging.debug('Start: remove column %s' % (column))
        self.coll.update_many(query, {'$unset': { column: ""}})
        logging.debug('End: remove column %s' % (column))

    def add_column(self, column, value, query={}):
        """Add column/field with value to collection"""
        logging.debug('Start: add column %s' % (column))
        self.coll.update_many(query, {'$set': { column: value}})
        logging.debug('End: add column %s' % (column))
        return self.get_column(column)

    def rename_column(self, old_name, new_name, query={}):
        """Rename single column/field in collection"""
        logging.debug('Start: rename column %s to %s' % (old_name, new_name))
        self.coll.update_many(query, {'$rename' : {old_name : new_name}})
        logging.debug('End: rename column %s to %s' % (old_name, new_name))

    def rename_columns(self, columns_map, query={}):
        """Rename list of columns to alternative"""
        logging.debug('Start: rename columns %s ' % (str(columns_map)))
        self.coll.update_many(query, {'$rename' : columns_map})
        logging.debug('End: rename column %s ' % (str(columns_map)))

    def transform_records(self, query, function, mode='full'):
        """Tranform each record in query"""
        logging.debug('Started transform function %s query %s mode %s' % (function.__qualname__, str(query), mode))
        if mode == 'full':
            n = 0
            operations = []
            for document in self.coll.find(query):
                n += 1
                new_document = function(document)
                operations.append(pymongo.ReplaceOne({'_id' : document['_id']}, new_document))
                if len(operations) == 1000:  
                    self.coll.bulk_write(operations)
                    operations = []            
            if len(operations) > 0:
                self.coll.bulk_write(operations)        
        logging.debug('Finished transform function %s query %s mode %s' % (function.__qualname__, str(query), mode))

    def join_columns(self, destination, columns=[], sep='', delete_joined=False, query={}):
        """Joins selected columns using separator and stores result in destination. Optionally deletes joined fields"""

        def do_join(record):
            return processing.join_fields(record, columns, destination, sep, delete_joined)
        self.transform_records(query, do_join)

    def split_column(self, column, sep='', limit='', positions=[], delete_original=True):
        """Split column no number of columns"""
        def do_split(record):
            return processing.split_field(record, sep, limit, positions, delete_original)

        self.transform_records(query, do_split)

    def transform_column(self, query, function, column, new_column=None, mode='full'):
        """Transform single column data"""
        logging.debug('Started transform of %s function %s query %s mode %s' % (column, function.__qualname__, str(query), mode))
        if not new_column: 
            new_column = column
        if mode == 'full':
            query[column] = {'$exists' : True}
            n = 0
            operations = []
            for row in self.coll.find(query, {'_id' : 1, column : 1}):
                n += 1
                new_value = function(glom.glom(row, column))
                operations.append(pymongo.UpdateOne({'_id' : row['_id']}, {'$set' : {new_column: new_value}}))
                if len(operations) == 1000:  
                    self.coll.bulk_write(operations)
                    operations = []            
            if len(operations) > 0:
                self.coll.bulk_write(operations)
        elif mode == 'distinct':
            query[column] = {'$exists' : True}
            distinct = self.coll.distinct(column, query)
            logging.debug('- finished distinct operation. Total %d' % (len(distinct)))
            n = 0
            operations = []
            for value in distinct:
                new_query = query.copy()
                n += 1
                new_value = function(value)
                new_query[column] = value
                operations.append(pymongo.UpdateMany(new_query, {'$set' : {new_column: new_value}}))
                if len(operations) == 100:
                    self.coll.bulk_write(operations)
                    operations = []
            if len(operations) > 0:
                self.coll.bulk_write(operations)
            
        logging.debug('Finished transform of %s function %s query %s mode %s' % (column, function.__qualname__, str(query), mode))





