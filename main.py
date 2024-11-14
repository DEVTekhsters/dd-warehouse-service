from fastapi import FastAPI
from app.routers import omd_router
from app.routers import ner_router

app = FastAPI()

# Include the entity router
app.include_router(omd_router.router, prefix="/omd")
app.include_router(ner_router.router, prefix="/ner")

@app.get("/")
async def root():
    return {"message": "Service Running"}
