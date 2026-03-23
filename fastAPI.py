# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 20:07:48 2026

@author: chenpengyyds
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🚀 优化 1：启动时【只加载】搜索用的列，不加载沉重的 CIF 文本
# 这样内存占用会从 500MB 瞬间降到 50MB 左右
FILE_NAME = "materials_v2.parquet"
SEARCH_COLS = ['Material ID', 'Formula', 'Predicted Formation Energy (eV/atom)', 'Band Gap (eV)', 'Space Group']

try:
    # 只读取必要的列，让 Railway 喘口气
    df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
    print(f"✅ 搜索索引加载成功：{len(df_search)} 条材料")
except Exception as e:
    print(f"❌ 加载失败: {e}")

@app.get("/search")
async def search(formula: str = "", energy: float = 0.5, page: int = 1):
    # 在轻量化索引中检索
    results = df_search[df_search['Predicted Formation Energy (eV/atom)'] <= energy]
    if formula:
        results = results[results['Formula'].str.contains(formula, case=False, na=False)]
    
    page_size = 30
    start = (page - 1) * page_size
    data = results.iloc[start:start + page_size].to_dict(orient="records")
    
    return {"total": len(results), "data": data}

@app.get("/get_cif")
async def get_cif(mpid: str):
    try:
        # 🚀 优化 2：只有当用户点下载时，才去磁盘里精准读取那一条 CIF
        # 这样平时完全不占内存
        single_df = pd.read_parquet(FILE_NAME, filters=[('Material ID', '==', mpid)])
        
        if single_df.empty:
            raise HTTPException(status_code=404, detail="未找到该材料")
            
        cif_text = single_df.iloc[0].get('cif_text', "")
        return {"mpid": mpid, "cif": cif_text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 🚀 优化 3：确保端口监听正确
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
