# python
from SearchEngine import lucene
from typing import *
from tqdm.auto import *
from concurrent.futures import *

# Lucene
from org.apache.lucene.analysis import Analyzer
from org.apache.lucene.document import Document
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, Term
from org.apache.lucene.store import Directory

# JAVA
from java.io import IOException
from java.lang import InterruptedException

class ThreadedIndexWriter(IndexWriter):
    def __init__(self, d:Directory, conf:IndexWriterConfig):
        super().__init__(d,conf)
        self.__threadpool = ThreadPoolExecutor()
        self.__futures = []
        
    def updateDocument(self, term:Term, doc:Document):
        self.__futures.append(self.__threadpool.submit(self.__job, doc, term))
    
    def addDocument(self, doc:Document):
        self.__futures.append(self.__threadpool.submit(self.__job, doc, None))
        
    def close(self, doWait:Optional[bool]=None):
        self.__finish()
        if doWait is not None:
            super().close(doWait)
        else:
            super().close()

    
    def rollback(self):
        self.__finish()
        super().rollback()
    
    def __finish(self):
        try:
            for __future in as_completed(self.__futures):
                err =  __future.exception()
                if err is not None:
                    raise err
            self.__threadpool.shutdown()
        except lucene.JavaError as jerr:
            expt = jerr.getJavaException()
            if isinstance(expt,InterruptedException):
                raise RuntimeError from jerr
        except Exception as pyerr:
            raise RuntimeError from pyerr
    
    def __job(self, doc:Document, delTerm:Optional[Term]):
        try:
            jvm_env = lucene.getVMEnv()
            jvm_env.attachCurrentThread()
            if (delTerm is not None):
                super().updateDocument(delTerm, doc)
            else:
                super().addDocument(doc)
            jvm_env.detachCurrentThread()
        except lucene.JavaError as jerr:
            err = jerr.getJavaException() 
            if isinstance(err, IOException):
                raise RuntimeError from jerr
            else:
                pass
        return None