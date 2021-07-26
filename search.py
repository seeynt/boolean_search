import re
import my_lib
import struct
import gzip
import sys
import array
import os
from sys import stdin

class Index:
    def __init__(self):
        path = 'varbyte_index.gz'
        self.pack_type = 'varbyte'
        
        if os.path.exists('simple9_index.gz'):
            path = 'simple9_index.gz'
            self.pack_type = 'simple9'
            
        self.index = {}
        with gzip.open(path, 'rb') as stream:
            while True:
                sb = stream.read(4)
                if len(sb) == 0:
                    break
                    
                size = struct.unpack('I', sb)[0]
                term = stream.read(size).decode('utf-8')
                
                sb = stream.read(4)
                size = struct.unpack('I', sb)[0]
                posting_list = stream.read(size)
                
                self.index[term] = posting_list
    
    def urls(self):
        path = 'docs_url.gz'
        
        self.docs_url = {}
        with gzip.open(path, 'rb') as stream:
            while True:
                sb = stream.read(4)
                if len(sb) == 0:
                    break
                    
                doc_id = struct.unpack('I', sb)[0]
                
                sb = stream.read(4)
                size = struct.unpack('I', sb)[0]
                url = stream.read(size).decode('utf-8')
                
                self.docs_url[doc_id] = url
        
        return self.docs_url
        
    def __getitem__(self, item):
        item = next(my_lib.pymorphy_tokenizer([item]))
        packed = self.index[item]
        posting_list = []
        
        if self.pack_type == 'varbyte':
            packer = my_lib.Varbyte()
            posting_list = packer.unpack(packed)
        elif self.pack_type == 'simple9':
            packer = my_lib.Simple9()
            posting_list = packer.unpack(packed)
            
        for i in range(len(posting_list) - 1):
            posting_list[i + 1] += posting_list[i]
            
        return posting_list

class Parser:
    def __init__(self, string, dictionary = None):
        self.string = string
        self.index = 0
        self.dictionary = dictionary

    def get_value(self):
        value = self.parse_expression()
        self.skip_whitespace()

        return value

    def peek(self):
        return self.string[self.index:self.index + 1]

    def has_next(self):
        return self.index < len(self.string)

    def is_next(self, value):
        return self.string[self.index:self.index + len(value)] == value

    def pop_if_next(self, value):
        if self.is_next(value):
            self.index += len(value)
            return True
        return False

    def pop_expected(self, value):
        if not self.pop_if_next(value):
            raise Exception("Expected '" + value + "' at index " + str(self.index))

    def skip_whitespace(self):
        while self.has_next():
            if self.peek() in ' \t\n\r':
                self.index += 1
            else:
                return

    def parse_expression(self):
        return self.parse_or()
    
    def parse_or(self):
        values = [self.parse_and()]
        
        while True:
            self.skip_whitespace()
            char = self.peek()
            
            if char == '|':
                self.index += 1
                values.append(self.parse_and())
            else:
                break
        
        value = values[0]
        
        for factor in values[1::]:
            value = my_lib.union(value, factor)
        return value

    def parse_and(self):
        values = [self.parse_parenthesis()]
            
        while True:
            self.skip_whitespace()
            char = self.peek()
                
            if char == '&':
                self.index += 1
                values.append(self.parse_parenthesis())
            else:
                break
                     
        value = values[0]
        
        for factor in values[1::]:
            value = my_lib.intersection(value, factor)
        return value

    def parse_parenthesis(self):
        self.skip_whitespace()
        char = self.peek()
        
        if char == '(':
            self.index += 1
            value = self.parse_expression()
            self.skip_whitespace()
            
            if self.peek() != ')':
                raise Exception("No closing parenthesis found at character " + str(self.index))
            self.index += 1
            return value
        else:
            return self.parse_not()

    def parse_arguments(self):
        args = []
        self.skip_whitespace()
        self.popExpected('(')
        while not self.pop_if_next(')'):
            self.skip_whitespace()
            if len(args) > 0:
                self.pop_expected(',')
                self.skip_whitespace()
            args.append(self.parse_expression())
            self.skip_whitespace()
        return args

    def parse_not(self):
        self.skip_whitespace()
        char = self.peek()
        
        if char == '!':
            self.index += 1
            return self.parse_parenthesis()
        else:
            return self.parse_value()

    def parse_value(self):
        self.skip_whitespace()
        var = []
        while self.has_next():
            char = self.peek()
            
            if char.lower() in 'абвгдеёжзийклмнопрстуфхцчшщъыьэюяabcdefghijklmnopqrstuvwxyz0123456789':
                var.append(char)
                self.index += 1
            else:
                break
                
        var = ''.join(var)
        return self.dictionary[var]

if __name__ == "__main__":
    index = Index()
    urls = index.urls()
    for line in stdin:
        if (line == '\n'):
            break
        result = [urls[i] for i in Parser(line, index).get_value()]
        print(line, len(result))
        for url in result:
            print(url)