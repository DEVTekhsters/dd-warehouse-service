from fastapi import FastAPI
from app.routers import omd_router
from app.routers import ner_router
from app.routers import unstructured_ner_router
from app.routers import pii_scanner_router
from middleware.custom_cors_middleware import CustomCORSMiddleware

origins = [
    "http://localhost:8000",
    "http://localhost:3000",
    "https://dev.gotrust.tech/",
    "https://preprod.gotrust.tech/",
    "https://portal.gotrust.tech/",
    "https://ucm.gotrust.tech/",
    "https://universal.gotrust.tech/",
    "https://pp-ucm.gotrust.tech/",
    "https://pp-universal.gotrust.tech/",
    "https://prod-ucm.gotrust.tech/",
    "https://prod-universal.gotrust.tech/"
 
]

app = FastAPI()

allow_origins = ["*"]
allow_methods = ["*"]
allow_headers = ["*"]
allow_credentials = True  # Set to True to allow credentials
 
#app.add_middleware(TimeoutMiddleware)  # Runs fifth
#app.add_middleware(ThrottlingMiddleware, max_requests=10000000, window=60)  # Runs third  
app.add_middleware(CustomCORSMiddleware, allow_origins=allow_origins, allow_methods=allow_methods, allow_headers=allow_headers, allow_credentials=allow_credentials)  # Runs first
 
 

# Include the entity router
app.include_router(omd_router.router, prefix="/omd")
app.include_router(ner_router.router, prefix="/ner")
app.include_router(unstructured_ner_router.router, prefix="/unstructured_ner")
app.include_router(pii_scanner_router.router, prefix="/instant-pii-scanner")



@app.get("/")
async def root():
    return {"message": "Service Running"}
