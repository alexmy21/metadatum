import os
import time
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from metadatum.controller import Controller
from metadatum.utils import Utils as utl


async def post_method(request):    
    t1 = time.perf_counter()
    data: dict = await request.json() 
    path = os.path.dirname(__file__) 
    response = Controller(path, data).run() 
    print(f'=== Execution time: {time.perf_counter() - t1}')

    return JSONResponse(response)

def startup():
    print('Starlette started')

routes = [
    Route('/post', post_method, methods=['POST']),
]

app = Starlette(debug=True, routes=routes, on_startup=[startup])