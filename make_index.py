import re
import argparse
import document_pb2
import my_lib
import struct
import gzip
import sys
import array
import os

class DocStreamReader:
    def __init__(self, paths):
        self.paths = paths

    def open_single(self, path):
        return gzip.open(path, 'rb') if path.endswith('.gz') else open(path, 'rb')

    def __iter__(self):
        for path in self.paths:
            with self.open_single(path) as stream:
                while True:
                    sb = stream.read(4)
                    if len(sb) == 0:
                        break

                    size = struct.unpack('i', sb)[0]
                    msg = stream.read(size)
                    doc = document_pb2.document()
                    doc.ParseFromString(msg)
                    yield doc


class IndexCreator:
    def __init__(self, pack_type='varbyte'):
        self.doc_to_url = {}
        self.index = {}
        self.pack_type = pack_type
            
        self.SPLIT_RGX = re.compile(r'\w+', re.U)
    
    def create_index(self, docs):
        for doc_id, doc in enumerate(docs):
            terms = set(my_lib.pymorphy_tokenizer(re.findall(self.SPLIT_RGX, doc.text.lower())))
            self.doc_to_url[doc_id] = doc.url
            
            for term in terms:
                if term in self.index.keys():
                    self.index[term].append(doc_id)
                else:
                    self.index[term] = array.array('I', [doc_id])
        
        return self.index, self.doc_to_url
    
    def compress_index(self):
        for term in self.index:
            posting_list = self.index[term]
            
            unpacked = [posting_list[0]]
            for i in range(len(posting_list) - 1):
                unpacked.append(posting_list[i + 1] - posting_list[i])
            
            packed = b''
            if self.pack_type == 'varbyte':
                packer = my_lib.Varbyte()
                packed = packer.pack(unpacked)
            elif self.pack_type == 'simple9':
                packer = my_lib.Simple9()
                packed = packer.pack(unpacked)
                
            self.index[term] = packed
            
    def save_index(self):
        path = 'index.gz'
        if self.pack_type == 'varbyte':
            path = 'varbyte_index.gz'
        elif self.pack_type == 'simple9':
            path = 'simple9_index.gz'
            
        with gzip.open(path, 'wb') as stream:
            for term in self.index:
                tb = bytes(term, 'utf-8')
                stream.write(struct.pack('I', len(tb)))
                stream.write(tb)
                stream.write(struct.pack('I', len(self.index[term])))
                stream.write(self.index[term])
                
        path = 'docs_url.gz'
        with gzip.open(path, 'wb') as stream:
            for doc_id in self.doc_to_url:
                stream.write(struct.pack('I', doc_id))
                ub = bytes(self.doc_to_url[doc_id], 'utf-8')
                stream.write(struct.pack('I', len(ub)))
                stream.write(ub)

def parse_command_line():
    parser = argparse.ArgumentParser(description='compressed documents reader')
    parser.add_argument('pack_type', nargs=1, help='simple9 | varbyte')
    parser.add_argument('paths', nargs='+', help='input files (.gz or plain) to process')
    return parser.parse_args()

if __name__ == "__main__":
	arguments = parse_command_line()
	pack_type = arguments.pack_type[0]
	paths = arguments.paths

	reader = DocStreamReader(paths)
	index = IndexCreator(pack_type)
	index.create_index(reader)
	index.compress_index()
	index.save_index()