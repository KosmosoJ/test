from fastapi import FastAPI
from routes.book import router as book_router


app = FastAPI()


app.include_router(book_router)


@app.get("")
async def index():
    return {"message": "Hello there!"}
