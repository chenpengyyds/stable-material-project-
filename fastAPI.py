# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 14:34:44 2026

@author: chenpengyyds
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

# 允许跨域（方便前端访问）
app.add_middleware(CORSMiddleware, allow_origins=["*"])

# 🚀 核心优化：启动时一次性把 Parquet 加载进内存（只读一次！）
df = pd.read_parquet("materials.parquet")

@app.get("/search")
async def search(formula: str = "", energy: float = 0.0, page: int = 1):
    # 极速内存过滤
    results = df[df['形成能 (eV/atom)'] <= energy]
    if formula:
        results = results[results['化学式'].str.contains(formula, case=False, na=False)]
    
    # 分页逻辑
    page_size = 50
    start = (page - 1) * page_size
    end = start + page_size
    
    total = len(results)
    data = results.iloc[start:end].to_dict(orient="records")
    
    return {"total": total, "data": data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)