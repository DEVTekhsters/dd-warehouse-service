from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from app.utils.response import *


class CustomCORSMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, allow_origins: list, allow_methods: list, allow_headers: list, allow_credentials: bool = True):
        super().__init__(app)
        self.allow_origins = allow_origins
        self.allow_methods = allow_methods
        self.allow_headers = allow_headers
        self.allow_credentials = allow_credentials
        
    async def dispatch(self, request: Request, call_next):
        try:
            print("Custom CORS Middleware executing...")
            if request.method == "OPTIONS":
                response = Response(status_code=204)
                if self.allow_credentials:
                    response.headers["Access-Control-Allow-Credentials"] = "true"
                response.headers["Access-Control-Allow-Origin"] = ", ".join(self.allow_origins) if self.allow_origins != ["*"] else "*"
                response.headers["Access-Control-Allow-Methods"] = ", ".join(self.allow_methods)
                response.headers["Access-Control-Allow-Headers"] = ", ".join(self.allow_headers)
                return response
            response = await call_next(request)
            if self.allow_credentials:
                response.headers["Access-Control-Allow-Credentials"] = "true"
            response.headers["Access-Control-Allow-Origin"] = ", ".join(self.allow_origins) if self.allow_origins != ["*"] else "*"
            response.headers["Access-Control-Allow-Methods"] = ", ".join(self.allow_methods)
            response.headers["Access-Control-Allow-Headers"] = ", ".join(self.allow_headers)
            print("Custom CORS Middleware completed.")
            # print("===================================CustomCORSMiddleware--before response===============================")
            return response
        except Exception as ex:
            # print("===================================CustomCORSMiddleware-error===============================")
            if isinstance(ex, HTTPException):
                return generate_error_response( status_code=ex.status_code, message="An error occurred", error=ex.detail)
            else:
                return generate_error_response( status_code=500, message="Internal Server Error", error=str(ex))
 