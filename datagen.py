#!/usr/bin/env python

from argparse import ArgumentParser
import sys
import os
from random import randint, uniform, choice
import string
from datetime import datetime, timedelta
import operator
import csv
from threading import Thread
import gzip

__author__ = 'franklinsijo'


class DataGen(object):
    ALLOWED_UNITS = ['K', 'M', 'G', 'T']
    ALLOWED_TYPES = ['TINYINT', 'SMALLINT', 'INT', 'BIGINT',
                     'FLOAT', 'DOUBLE', 'DECIMAL',
                     'VARCHAR', 'TEXT',
                     'DATE', 'TIMESTAMP']
    SIZE_PER_FILE = 10 * 1024 * 1024  # 10 MB
    CONSTRAINTS = {
        'DECIMAL_PRECISION': 5,
        'DECIMAL_SCALE': 2,
        'VARCHAR_MIN': 6,
        'VARCHAR_MAX': 20,
        'TEXT_MIN': 21,
        'TEXT_MAX': 99,
        'DAYS_AGO': 1095,
        'DATE_FORMAT': '%Y-%m-%d',
        'TIMESTAMP_FORMAT': '%Y-%m-%d %H:%M:%S'
    }
    FUNC = {
        'NUMBER': "randint(1, 2**2**6)",
        'TINYINT': "randint(1, 2**2**3)",
        'SMALLINT': "randint(2**2**3, 2**2**4)",
        'INT': "randint(2**2**4, 2**2**5)",
        'BIGINT': "randint(2**2**5, 2**2**6)",
        'FLOAT': "round(uniform(1, 100), randint(1, 6))",
        'DOUBLE': "round(uniform(1, 1000), randint(7, 15))",
        'DECIMAL': "format(uniform(int('1' + '0' * (self.CONSTRAINTS['DECIMAL_PRECISION'] - self.CONSTRAINTS['DECIMAL_SCALE'] - 1)), int('1' + '0' * (self.CONSTRAINTS['DECIMAL_PRECISION'] - self.CONSTRAINTS['DECIMAL_SCALE'])) - 1), '.' + str(self.CONSTRAINTS['DECIMAL_SCALE']) + 'f')",
        'VARCHAR': "''.join(choice(string.ascii_lowercase) for _ in xrange(randint(self.CONSTRAINTS['VARCHAR_MIN'], self.CONSTRAINTS['VARCHAR_MAX'])))",
        'TEXT': "''.join(choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in xrange(randint(self.CONSTRAINTS['TEXT_MIN'], self.CONSTRAINTS['TEXT_MAX'])))",
        'DATE': "datetime.strftime(datetime.today() - timedelta(randint(1, self.CONSTRAINTS['DAYS_AGO'])), self.CONSTRAINTS['DATE_FORMAT'])",
        'TIMESTAMP': "datetime.strftime(datetime.now() - timedelta(days=randint(1, self.CONSTRAINTS['DAYS_AGO']), hours=randint(1, 23), minutes=randint(1, 59), seconds=randint(1, 59)), self.CONSTRAINTS['TIMESTAMP_FORMAT'])"
    }

    def __init__(self, args):
        self.delimiter = args.DELIMITER
        if args.DELIMITER == 't': self.delimiter = '\t'
                
        self.num_records = args.NUM_RECORDS
        self.use_size = False
        if args.SIZE:
            self.use_size = True
            try:
                self.size = int(args.SIZE)
            except ValueError:
                unit = args.SIZE[-1:].upper()
                try:
                    size = int(args.SIZE[:-1])
                    if unit in self.ALLOWED_UNITS:
                        if unit == 'K':
                            self.size = size * 1024
                        elif unit == 'M':
                            self.size = size * 1024 ** 2
                        elif unit == 'G':
                            self.size = size * 1024 ** 3
                        elif unit == 'T':
                            self.size = size * 1024 ** 4
                    else:
                        raise Exception("Invalid Size Argument. Valid Units are K, M, G, T")
                except ValueError:
                    raise Exception(
                        "Invalid Size Argument. Size argument must be of the pattern <digits><unit>. Ex: 1000G")
            self.size = int(round(self.size, -1))  # Rounding it to the nearest 10

        self.num_columns = args.NUM_COLUMNS

        if args.NUM_FILES:
            self.num_files = args.NUM_FILES
        else:
            self.num_files = int(self.num_records / 100000) if self.num_records > 100000 else 1
            if self.use_size:
                self.num_files = int(self.size / self.SIZE_PER_FILE) if self.size > self.SIZE_PER_FILE else 1

        self.target_path = os.path.abspath(args.TARGET_PATH)
        self.file_prefix = args.FILE_PREFIX
        self.file_suffix = args.FILE_SUFFIX

        self.compression_enabled = args.compress
        if self.compression_enabled:
            self.file_suffix = self.file_suffix + '.gz'

        self.concurrency_enabled  = False
        if args.NUM_THREADS:
            self.concurrency_enabled = True
            self.thread_count = args.NUM_THREADS

        self.headers = ['field' + str(n) for n in range(1, self.num_columns + 1)]
        self.field_list = self.get_fieldlist()
        self.check_storage()

    def check_storage(self):
        """
        Pre-check the target directory for available free space and Write permissions
        """
        stats = os.statvfs(self.target_path)
        free_space = int(round(stats.f_bsize * stats.f_bavail, -1))
        if self.size > free_space:
            raise Exception("Insufficient Space. Required: %d Bytes, Available: %d Bytes" % (self.size, free_space))
        try:
            tmpfile = os.path.join(self.target_path, 'datagen.tmp')
            with open(tmpfile, 'w') as tfile:
                tfile.write('Test File for Write Access')
            tfile.close()
            os.remove(tmpfile)
        except IOError:
            raise Exception('Permission Denied: %s' % self.target_path)

    def update_constraints(self, constraints):
        def integerize(i, key):
            try:
                integer_value = int(i)
                return integer_value
            except ValueError:
                raise Exception('%s must be an integer' % key)

        for k, v in sorted(constraints.items(), key=operator.itemgetter(0), reverse=True):
            if k.upper() not in ['DATE_FORMAT', 'TIMESTAMP_FORMAT']:
                v = integerize(v, k)

            if k.upper() == 'DECIMAL_PRECISION':
                if v < self.CONSTRAINTS['DECIMAL_SCALE']:
                    raise Exception(
                        'DECIMAL_PRECISION constraint cannot be less than or equal to DECIMAL_SCALE')
            elif k.upper() == 'TEXT_MAX':
                if v < self.CONSTRAINTS['TEXT_MIN']:
                    raise Exception('TEXT_MAX constraint cannot be less than or equal to TEXT_MIN')
            elif k.upper() == 'VARCHAR_MAX':
                if v < self.CONSTRAINTS['VARCHAR_MIN']:
                    raise Exception('VARCHAR_MAX constraint cannot be less than or equal to VARCHAR_MIN')

            self.CONSTRAINTS[k.upper()] = v

    def get_fieldlist(self):
        return [self.ALLOWED_TYPES[randint(0, len(self.ALLOWED_TYPES) - 1)] for _ in range(self.num_columns)]

    def write2file(self, fp, max_per_file):        
        nrows_threshold = 100000

        def writer(nrows):
            rows = []
            for _ in range(nrows):
                rows.append([eval(self.FUNC[f]) for f in self.FIELDLIST])
            if self.compression_enabled:
                datafile = gzip.open(fp, 'ab')
            else:
                datafile = open(fp, 'ab')
            csvwriter = csv.writer(datafile, delimiter=self.delimiter)
            csvwriter.writerows(rows)
            datafile.close()

        if self.use_size:
            """
            When generating data based on a size limit, 
            the resultant sum of the size of all files generated
            will be equal to greater than the size requested.
            """
            write_more = True
            while write_more:
                writer(nrows_threshold)
                fsize = os.path.getsize(fp)
                if fsize >= max_per_file:
                    write_more = False
        else:
            while max_per_file:
                if max_per_file > nrows_threshold:
                    writer(nrows_threshold)
                    max_per_file = max_per_file - nrows_threshold
                else:
                    writer(max_per_file)
                    max_per_file = 0

    def generate(self):
        # from pprint import pprint
        # pprint(self.__dict__)
        """
        If the number of files is set by the user, 
        per file limits, either in size or in record count
        is set using this max_per_file parameter
        """
        if self.use_size:
            max_per_file = int(self.size / self.num_files)
        else:
            max_per_file = int(self.num_records / self.num_files)

        if self.concurrency_enabled:
            stop = 1
            while self.num_files:
                start = stop
                if self.thread_count >= self.num_files:
                    stop = stop + self.num_files
                    self.num_files = 0
                else:
                    stop = stop + self.thread_count
                    self.num_files = self.num_files - self.thread_count
                threads = []
                print(start, stop, self.num_files)
                for f in range(start, stop):
                    fp = os.path.join(self.target_path, self.file_prefix + str(f) + self.file_suffix)
                    threads.append(Thread(target=self.write2file, args=(fp, max_per_file,)))
                [thread.start() for thread in threads]
                [thread.join() for thread in threads]
        else:
            for f in range(1, self.num_files + 1):
                fp = os.path.join(self.target_path, self.file_prefix + str(f) + self.file_suffix)
                self.write2file(fp, max_per_file)


if __name__ == '__main__':
    argparser = ArgumentParser()
    argparser.add_argument("-d", "--delimiter",
                           dest="DELIMITER",
                           type=str,
                           default=",",
                           help="delimiter to separate the columns.")
    argparser.add_argument("-r", "--records",
                           dest="NUM_RECORDS",
                           type=int,
                           default=1000,
                           help="total number of records to generate. Will not be used if size parameter is specified.")
    argparser.add_argument("-s", "--size",
                           dest="SIZE",
                           help="total size of data to generate. Takes precedence over records parameter.")                           
    argparser.add_argument("-c", "--columns",
                           dest="NUM_COLUMNS",
                           type=int,
                           default=10,
                           help="number of required columns")
    argparser.add_argument("-f", "--files",
                           dest="NUM_FILES",
                           type=int,
                           help="number of files to generate")
    argparser.add_argument("-o", "--target-dir",
                           dest="TARGET_PATH",
                           default=os.path.dirname(__file__),
                           help="path to store the generated files")
    argparser.add_argument("--prefix",
                           dest="FILE_PREFIX",
                           default="datagen_file_",
                           help="filenames should start with")
    argparser.add_argument("--suffix",
                           dest="FILE_SUFFIX",
                           default="",
                           help="filenames should end with")
    argparser.add_argument("--compress",
                           action="store_true",
                           help="Gzip compress the generated files")
    argparser.add_argument("-t", "--threads",
                           dest="NUM_THREADS",
                           type=int,
                           help="number of threads to use")
    argparser.add_argument("--constraints",
                           dest="DATA_CONSTRAINTS",
                           type=dict,
                           help="dictionary of custom data format and length. Allowed keys are "
                                "DECIMAL_PRECISION, DECIMAL_SCALE, VARCHAR_MIN, VARCHAR_MAX, TEXT_MIN, TEXT_MAX,"
                                "DAYS_AGO, DATE_FORMAT, TIMESTAMP_FORMAT.")
    argparser.parse_args()
    datagen = DataGen(argparser.parse_args())
    datagen.generate()
