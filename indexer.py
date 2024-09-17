#cudf
cudf_enabled = False
try:
    import cudf, cudf.pandas
    cudf.pandas.install()
    cudf_enabled = True
except ImportError:
    cudf_enabled = False

# python
from SearchEngine import lucene
import pandas as pd
import os, magic, json, ipaddress
from typing import *
from pandas.api.types import (
    is_string_dtype,
    is_integer_dtype,
    is_float_dtype,
    is_datetime64_any_dtype,
    is_object_dtype,
)
from tqdm.auto import *
from tqdm.contrib.concurrent import thread_map
from .threadedindexwriter import ThreadedIndexWriter


# Lucene
from org.apache.lucene.document import (
    Document,
    Field,
    StringField,
    TextField,
    StoredField,
    NumericDocValuesField,
    DoubleDocValuesField,
    DateTools,
    InetAddressPoint,
)
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory

# JAVA
from java.io import File
from java.util import Date
from java.net import InetAddress

def is_ipaddress_dtype(s:pd.Series):
    for obj in s:
        if not isinstance(obj,(ipaddress.IPv4Address,ipaddress.IPv6Address)):
            return False
    return True

class DataFrameIndexer:
    def __init__(
        self,
        dataframe: pd.DataFrame,
        index_path: str,
        writer_cfg:IndexWriterConfig,
        use_thread: bool = False,
        raw_data_field: str | None = "message",
        *args,
        **kwargs
    ):
        self.__data = dataframe
        self.indexPath = File(os.path.abspath(index_path)).toPath()
        self.indexDir = FSDirectory.open(self.indexPath)    

        if raw_data_field is not None:
            global cudf_enabled
            if cudf_enabled:
                tmp = cudf.from_pandas(self.__data)
                lmessages = json.loads(tmp.to_json(engine='cudf', orient='records'))
                self.__data.loc[:,raw_data_field] = lmessages
                self.__data.loc[:,raw_data_field] = self.__data[raw_data_field].astype('str')
                del tmp
            else:
                lmessages = json.loads(self.__data.to_json(orient='records'))
                self.__data.loc[:,raw_data_field] = lmessages
                self.__data.loc[:,raw_data_field] = self.__data[raw_data_field].astype('str')
        
        print(f"[+] 원본 데이터 읽기 완료.")
        
        if use_thread:
            self.__indexWriter = ThreadedIndexWriter(self.indexDir, writer_cfg)
        else:
            self.__indexWriter = IndexWriter(self.indexDir, writer_cfg)
            
    def __proc_init(self):
        jvm = lucene.getVMEnv()
        jvm.attachCurrentThread()
    
    def __generateField(
        self,
        column_name: str,
        idx,
        prop,
        raw_data_field: str | None = "message",
        *args,
        **kwargs
    ):
        try:
            col = self.__data[column_name]

            if (raw_data_field is not None) and (column_name == raw_data_field):
                return [TextField(column_name, col.loc[idx], prop)]

            # string
            if is_string_dtype(col):
                return [StringField(column_name, col.loc[idx], prop)]
            # Integer
            elif is_integer_dtype(col):
                return [
                    StoredField(column_name, int(col.loc[idx])),
                    NumericDocValuesField(column_name, int(col.loc[idx])),
                ]
            # Float
            elif is_float_dtype(col):
                return [
                    StoredField(column_name, float(col.loc[idx])),
                    DoubleDocValuesField(column_name, float(col.loc[idx])),
                ]
            # Datetime
            elif is_datetime64_any_dtype(col):
                jtime = int(col.loc[idx].timestamp() * 1000 )
                jdate = Date(jtime)
                dt = col.loc[idx].strftime("%Y-%m-%d %H:%M:%S.%f")
                return [
                    StringField(column_name, dt, prop),
                    NumericDocValuesField(column_name, jtime)
                ]
            # IP Address
            elif is_ipaddress_dtype(col):
                str_ip = str(col.loc[idx])
                return [InetAddressPoint(column_name, InetAddress.getByName(str_ip))]
            else:
                contents = str(col.loc[idx])
                return [TextField(column_name, contents, prop)]

        except KeyError as error:
            raise ValueError from error


    def makeIndex(self):
        docs = list(thread_map(self.__make_index_work, self.__data.index))
        
        self.__indexWriter.deleteAll()
        for doc in docs:
            self.__indexWriter.addDocument(doc)
        self.__indexWriter.commit()
        self.__indexWriter.close()
        
    def __make_index_work(self, idx):
        try:
            jvm_env = lucene.getVMEnv()
            jvm_env.attachCurrentThread()
            doc = Document()
            
            for col in self.__data.columns:
                fields = self.__generateField(col, idx, Field.Store.YES)
                for field in fields:
                    doc.add(field)
            return doc
        except Exception as err:
            raise RuntimeError from err