from fastapi import APIRouter
from dal import Queries
router = APIRouter()

@router.get("/")
async def root():
    return {"message": "Fast api is running"}

@router.get("/all")
async def get_all_podcasts():
    queries = Queries()
    return queries.all_podcasts()

