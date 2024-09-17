# Lucene classes
from org.apache.lucene.analysis import Analyzer, TokenStream, StopFilter, TokenFilter
from org.apache.lucene.analysis.core import StopAnalyzer
from org.apache.lucene.analysis.classic import ClassicTokenizer
from org.apache.lucene.analysis.tokenattributes import TypeAttribute, CharTermAttribute
from org.apache.lucene.analysis.standard import StandardTokenizer
from org.apache.lucene.util import Version

# PyLucene Python Subclasses
from org.apache.pylucene.analysis import (
    PythonTokenFilter,
    PythonTokenStream,
    PythonAnalyzer
)

# Java Class
from java.io import Reader


class PyStandardFilter(PythonTokenFilter):
    def __init__(self, matchedVersion, tokenin: PythonTokenStream, *args, **kwargs):
        super(PyStandardFilter, self).__init__(tokenin)
        self.matchVersion = matchedVersion
        self.__APOSTROPHE_TYPE = ClassicTokenizer.TOKEN_TYPES[
            ClassicTokenizer.APOSTROPHE
        ]
        self.__ACRONYM_TYPE = ClassicTokenizer.TOKEN_TYPES[ClassicTokenizer.ACRONYM]
        self.typeAtt = super().addAttribute(TypeAttribute.class_)
        self.termAtt = super().addAttribute(CharTermAttribute.class_)
        self.__input = tokenin

    def incrementToken(self) -> bool:
        try:
            if not self.__input.incrementToken():
                return False

            buffer = self.termAtt.buffer()
            bufferLength = len(self.termAtt.toString())
            tp = self.typeAtt.type()

            if (
                (tp == self.__APOSTROPHE_TYPE)
                and (bufferLength >= 2)
                and (buffer[bufferLength - 2] == "'")
                and (
                    (buffer[bufferLength - 1] == "s")
                    or (buffer[bufferLength - 1] == "S")
                )
            ):
                self.termAtt.setLength(bufferLength - 2)
            elif tp == self.__ACRONYM_TYPE:
                upto = 0
                for i in range(bufferLength):
                    c = buffer[i]
                    if c != ".":
                        buffer[upto] = c
                        upto += 1
                self.termAtt.setLength(upto)

            return True
        except Exception as err:
            raise OSError from err


class CaseSensitiveStandardAnalyzer(PythonAnalyzer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def createComponents(self, fieldName):
        source = StandardTokenizer()
        result = PyStandardFilter(Version.LUCENE_9_10_0, source)
        return Analyzer.TokenStreamComponents(source, result)

    def tokenStream(fieldName, reader: Reader) -> TokenStream:
        tokenizer = StandardTokenizer(reader)
        filterStream = PyStandardFilter(Version.LUCENE_9_10_0, tokenizer)
        stream = StopFilter(
            True, filterStream, StopAnalyzer.ENGLISH_STOP_WORDS_SET, True
        )
        return stream
