from fastapi import FastAPI
from app.routers import omd_router
from app.routers import ner_router
from app.routers import unstructured_ner_router

app = FastAPI()

# Include the entity router
app.include_router(omd_router.router, prefix="/omd")
app.include_router(ner_router.router, prefix="/ner")
app.include_router(unstructured_ner_router.router, prefix="/unstructured_ner")



@app.get("/")
async def root():
    return {"message": "Service Running"}
