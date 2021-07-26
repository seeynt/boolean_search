import struct
import array
import pymorphy2

PYMORPHY_CACHE = {}
morph = pymorphy2.MorphAnalyzer()

#______help funcs______

def pymorphy_tokenizer(text):
    global PYMORPHY_CACHE

    for word in text:
        word_hash = hash(word)
        if word_hash not in PYMORPHY_CACHE:
            PYMORPHY_CACHE[word_hash] = morph.parse(word)[0].normal_form            
        yield PYMORPHY_CACHE[word_hash]
        
def union(arr1, arr2):
    m, n, i, j = len(arr1), len(arr2), 0, 0
    result = []
    
    while i < m and j < n: 
        if arr1[i] < arr2[j]: 
            result.append(arr1[i]) 
            i += 1
        elif arr2[j] < arr1[i]: 
            result.append(arr2[j]) 
            j+= 1
        else: 
            result.append(arr2[j]) 
            j += 1
            i += 1
  
    while i < m: 
        result.append(arr1[i]) 
        i += 1
    while j < n: 
        result.append(arr2[j]) 
        j += 1
        
    return result
        
def intersection(arr1, arr2): 
    m, n, i, j = len(arr1), len(arr2), 0, 0
    result = []
    
    while i < m and j < n: 
        if arr1[i] < arr2[j]: 
            i += 1
        elif arr2[j] < arr1[i]: 
            j += 1
        else: 
            result.append(arr2[j]) 
            j += 1
            i += 1
            
    return result

#_______coders_______

class Varbyte:
    def __init__(self):
        pass
    
    def pack(self, numbers):
        bytes_list = []
        for number in numbers:
            bytes_list.append(self.encode_number(number))
        return b''.join(bytes_list)
    
    def encode_number(self, number):
        bytes_list = []
        while True:
            bytes_list.insert(0, number % 128)
            if number < 128:
                break
            number = number // 128
        bytes_list[-1] += 128
        return struct.pack('%dB' % len(bytes_list), *bytes_list)
        
    def unpack(self, packed):
        n = 0
        numbers = []
        bytestream = struct.unpack('%dB' % len(packed), packed)
        for byte in bytestream:
            if byte < 128:
                n = 128 * n + byte
            else:
                n = 128 * n + (byte - 128)
                numbers.append(n)
                n = 0
        return numbers
    
#почему-то он не сжимает сильно лучше Varbyte...
class Simple9:
    def __init__(self):
        self.code = {1 : 0, 2 : 1, 3 : 2, 4 : 3, 5 : 4, 7 : 5, 9 : 6, 14 : 7, 28 : 8}
        self.coded = {0 : 1, 1 : 2, 2 : 3, 3 : 4, 4 : 5, 5 : 7, 6 : 9, 7 : 14, 8 : 28}

    def encode_chunk(self, numbers):
        n, length = 0, len(numbers)
        n = int(self.code[length] << 28)
        for i in range(length):
            n += numbers[i] << ((28 // length) * i)
        
        return struct.pack('I', n)
    
    def pack(self, numbers):
        bytes_list = []
        
        n, length = 0, len(numbers)
        while n < length:
            if n + 28 <= length and max(numbers[n:n + 28:]) <= 1:
                bytes_list.append(self.encode_chunk(numbers[n:n + 28:]))
                n += 28
            elif n + 14 <= length and max(numbers[n:n + 14:]) <= 3:
                bytes_list.append(self.encode_chunk(numbers[n:n + 14:]))
                n += 14
            elif n + 9 <= length and max(numbers[n:n + 9:]) <= 7:
                bytes_list.append(self.encode_chunk(numbers[n:n + 9:]))
                n += 9
            elif n + 7 <= length and max(numbers[n:n + 7:]) <= 15:
                bytes_list.append(self.encode_chunk(numbers[n:n + 7:]))
                n += 7
            elif n + 5 <= length and max(numbers[n:n + 5:]) <= 31:
                bytes_list.append(self.encode_chunk(numbers[n:n + 5:]))
                n += 5
            elif n + 4 <= length and max(numbers[n:n + 4:]) <= 127:
                bytes_list.append(self.encode_chunk(numbers[n:n + 4:]))
                n += 4
            elif n + 3 <= length and max(numbers[n:n + 3:]) <= 511:
                bytes_list.append(self.encode_chunk(numbers[n:n + 3:]))
                n += 3
            elif n + 2 <= length and max(numbers[n:n + 2:]) <= 2**14 - 1:
                bytes_list.append(self.encode_chunk(numbers[n:n + 2:]))
                n += 2
            else:
                bytes_list.append(self.encode_chunk([numbers[n]]))
                n += 1
         
        return b''.join(bytes_list)
        
    def unpack(self, packed):
        numbers = []
        n = 0
        stream = struct.unpack('%dI' % (len(packed) // 4), packed)
        for chunk in stream:
            length = self.coded[chunk >> 28]
            for i in range(length):
                n = (chunk >> ((28 // length) * i)) % (1 << (28 // length))
                numbers.append(n)
                
        return numbers