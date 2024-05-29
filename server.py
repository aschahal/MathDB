import threading 
from collections import OrderedDict
import mathdb_pb2
import mathdb_pb2_grpc
import traceback
from concurrent import futures
import grpc

class MathCache:
    def __init__(self):
        self.cache = {}
        self.lru_cache = OrderedDict()
        self.lock = threading.Lock()

    def Set(self, key, value):
        with self.lock:
            self.cache[key] = value
            self.lru_cache.clear()

    def Get(self, key):
        return  self.cache[key]
    
    def Add(self, key_a, key_b):
        return self._operation('add', key_a, key_b, lambda a, b: a + b)
    
    def Sub(self, key_a, key_b):
        return self._operation('sub', key_a, key_b, lambda a, b: a - b)
    
    def Mult(self, key_a, key_b):
        return self._operation('mult', key_a, key_b, lambda a, b: a * b)
    
    def Div(self, key_a, key_b):
        return self._operation('div', key_a, key_b, lambda a, b: a / b)
    
    def _operation(self, op_name, key_a, key_b, operation):
        with self.lock:
            cache_key = (op_name, key_a, key_b)
            
            if cache_key in self.lru_cache:
                result = self.lru_cache[cache_key]
                self.lru_cache.move_to_end(cache_key)
                return result, True
            
            a, b = self.Get(key_a), self.Get(key_b)
            result = operation(a, b)
            self._update_lru_cache(cache_key, result)
            return result, False
    
    def _update_lru_cache(self, cache_key, result):
        if len(self.lru_cache) >= 10:
            self.lru_cache.popitem(last=False)
        self.lru_cache[cache_key] = result

class MathDb(mathdb_pb2_grpc.MathDbServicer):
    def __init__(self):
        self.cache = MathCache()

    def Set(self, request, context):
        try:
                self.cache.Set(request.key, request.value)
                return mathdb_pb2.SetResponse(error="")
        except Exception:
            return mathdb_pb2.SetResponse(error=traceback.format_exc())
        
    def Get(self, request, context):
        try:
            value = self.cache.Get(request.key)
            return mathdb_pb2.GetResponse(value=value, error="")
        except Exception:
            return mathdb_pb2.GetResponse(error=traceback.format_exc())
        
    def Add(self, request, context):
        try:
            result, cache_hit = self.cache.Add(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=result, cache_hit=cache_hit, error="")
        except Exception:
            return mathdb_pb2.BinaryOpResponse(error=traceback.format_exc(), value = 0, cache_hit = False)
    
    def Sub(self, request, context):
        try:
            result, cache_hit = self.cache.Sub(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=result, cache_hit=cache_hit, error="")
        except Exception:
            return mathdb_pb2.BinaryOpResponse(error=traceback.format_exc(), value = 0, cache_hit = False)
    
    def Mult(self, request, context):
        try:
            result, cache_hit = self.cache.Mult(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=result, cache_hit=cache_hit, error="")
        except Exception:
            return mathdb_pb2.BinaryOpResponse(error=traceback.format_exc(), value = 0, cache_hit = False)
    
    def Div(self, request, context):
        try:
            result, cache_hit = self.cache.Div(request.key_a, request.key_b)
            return mathdb_pb2.BinaryOpResponse(value=result, cache_hit=cache_hit, error="")
        except Exception:
            return mathdb_pb2.BinaryOpResponse(error=traceback.format_exc(), value = 0, cache_hit = False)

if __name__ == "__main__":
  server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
  mathdb_pb2_grpc.add_MathDbServicer_to_server(MathDb(), server)
  server.add_insecure_port("[::]:5440", )
  server.start()
  server.wait_for_termination() 

    

    
    
