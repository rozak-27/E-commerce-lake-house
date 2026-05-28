"""
FastAPI App
===========
Entry point untuk Analytics API
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.analytics import router

app = FastAPI(
    title="E-Commerce Lakehouse API",
    description="Analytics API — query data dari Trino",
    version="1.0.0",
)

# CORS — allow dashboard (React) akses API ini
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

@app.get("/")
def root():
    return {"status": "ok", "message": "E-Commerce Lakehouse API"}

@app.get("/health")
def health():
    return {"status": "healthy"}