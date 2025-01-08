import time
from typing import List, Optional
from fastapi.responses import JSONResponse # type: ignore
 
def generate_success_response(status_code: int, message: str, count: Optional[int]=None, data: Optional[List]=None):
    response_content = {
        "success": True,
        "status_code": status_code,
        "message": message,
        "result": {},
        "time": int(time.time() * 1000)
    }    
    if count is not None:
        if data is not None:
            response_content["result"]['data'] = data
        response_content["result"]['count'] = count
    else:
        response_content["result"]['data'] = data or []
    return JSONResponse(
        status_code=status_code,
        content=response_content
    )
 
def generate_error_response(status_code: int, message: str, error: str):
    return JSONResponse(
        status_code=status_code,
        content={
            "success": False,
            "status_code": status_code,
            "message": message,
            "result": {
                "error": error
            },
            "time": int(time.time() * 1000)  # current time in milliseconds
        }
    )
 