#cudf
try:
    import cudf.pandas
    cudf.pandas.install()
except ImportError:
    pass

import pandas as pd
import os.path

# lucene
from org.apache.lucene.queryparser.flexible.standard import StandardQueryParser
from org.apache.lucene.index import IndexReader, DirectoryReader
from org.apache.lucene.search import (
    IndexSearcher,
    TopDocs,
    ScoreDoc,
    Sort,
    TopFieldDocs,
    SortField,
)
from org.apache.lucene.analysis import Analyzer
from org.apache.lucene.store import FSDirectory
from lucene import JavaError

# JAVA
from java.io import File
from java.nio.file import Path


class DataFrameSearcher:
    def __init__(
        self,
        analyzer: Analyzer,
        index_path: str,
        ntop: int,
        allow_leading_wildcard: bool = True,
        defaultFieldName: str = "message",
        *args,
        **kwargs,
    ):
        if (analyzer is None) or (ntop <= 0):
            raise ValueError

        self.__analyzer = analyzer
        self.__query_parser = StandardQueryParser(self.__analyzer)
        self.__query_parser.setAllowLeadingWildcard(allow_leading_wildcard)
        self.__directory = FSDirectory.open(File(os.path.abspath(index_path)).toPath())
        self.__indexReader = DirectoryReader.open(self.__directory)
        self.__searcher = IndexSearcher(self.__indexReader)
        self.__ntop = ntop
        self.__default_fieldName = defaultFieldName

    def __del__(self):
        self.__indexReader.close()

    def search(self, query: str, *args, **kwargs) -> pd.DataFrame:
        try:
            __q = self.__query_parser.parse(query, self.__default_fieldName)
            n_of_result = self.__searcher.count(__q)
            print(f"총 {n_of_result}건 검색됨.")

            hits = None
            res = []

            for i in range(0, n_of_result, self.__ntop):
                if not hits:
                    hits = self.__searcher.search(__q, self.__ntop)
                else:
                    hits = self.__searcher.searchAfter(
                        hits.scoreDocs[-1], __q, self.__ntop
                    )

                for s in hits.scoreDocs:
                    rdoc = self.__searcher.doc(s.doc)
                    fields = [f.name() for f in rdoc.getFields()]
                    values = [rdoc.get(fn) for fn in fields]
                    __rec = dict(zip(fields, values))
                    res.append(__rec)
            del hits
            return pd.DataFrame(res)
        except JavaError as jerr:
            if "LEADING_WILDCARD_NOT_ALLOWED" in str(jerr):
                raise SyntaxError("LEADING_WILDCARD_NOT_ALLOWED") from jerr
            elif "INVALID_SYNTAX_CANNOT_PARSE" in str(jerr):
                raise SyntaxError("INVALID_SYNTAX_CANNOT_PARSE") from jerr
            else:
                raise RuntimeError from jerr
