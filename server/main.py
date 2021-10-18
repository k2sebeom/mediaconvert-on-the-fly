from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import boto3
from os import path

s3 = boto3.client("s3")
app = FastAPI()

app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"]
)

@app.post("/file/")
async def create_file(file: UploadFile = File(...)):
    body = await file.read()
    s3.put_object(Bucket="{Bucket Name}", Key=file.filename, Body=body)
    return { "filesize": len(body) }
    
@app.get("/url/")
async def get_url(file_name: str):
    base_name = path.splitext(file_name)[0]
    try:
        s3.get_object(Bucket='{Bucket Name}', Key=f"{base_name}/master.m3u8")
        return {'url': f"{CF domain}/{base_name}/master.m3u8"}
    except Exception:
        return {'url': None}

