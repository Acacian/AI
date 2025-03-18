import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI

# env load
load_dotenv(dotenv_path=".env")
app = FastAPI()

if __name__ == "__main__":
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000)
