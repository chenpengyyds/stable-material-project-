# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 20:07:48 2026

@author: chenpengyyds
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

# 允许跨域（必须保留，否则网页无法读取）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🚀 加载 21万条的全量超级油箱
# 确保你的文件名和 Parquet2.py 生成的一致
df = pd.read_parquet("materials_v2.parquet")

# --- 接口 1：极速列表搜索 ---
@app.get("/search")
async def search(formula: str = "", energy: float = 0.0, page: int = 1):
    # 💡 关键修正：这里的列名必须匹配 CSV 里的 'Predicted Formation Energy (eV/atom)'
    results = df[df['Predicted Formation Energy (eV/atom)'] <= energy]
    
    # 💡 关键修正：搜索列改为 'Formula'
    if formula:
        results = results[results['Formula'].str.contains(formula, case=False, na=False)]
    
    page_size = 30
    start = (page - 1) * page_size
    
    # 💡 全量展示：这里定义要在网页表格里展示的列
    # 你可以根据 CSV 里的实际表头继续往里加，比如 'Space Group', 'Crystal System'
    columns_to_return = [
        'Material ID', 
        'Formula', 
        'Predicted Formation Energy (eV/atom)', 
        'Band Gap (eV)', 
        'Space Group'
    ]
    
    # 只提取存在的列，避开体积庞大的 'cif_text'
    available_cols = [col for col in columns_to_return if col in results.columns]
    
    # 执行切片和格式转换
    data = results.iloc[start:start + page_size][available_cols].to_dict(orient="records")
    
    return {
        "total": len(results), 
        "data": data,
        "page": page
    }

# --- 接口 2：精准 CIF 下载 ---
@app.get("/get_cif")
async def get_cif(mpid: str):
    # 💡 关键修正：查找列改为 'Material ID'
    material = df[df['Material ID'] == mpid]
    
    if material.empty:
        raise HTTPException(status_code=404, detail="未找到该材料 ID")
        
    cif_text = material.iloc[0].get('cif_text', "")
    
    if not cif_text:
        raise HTTPException(status_code=404, detail="该材料暂无对应的 CIF 结构文件")
    
    return {"mpid": mpid, "cif": cif_text}