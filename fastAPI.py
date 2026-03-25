from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import urllib.request
import threading
import duckdb

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FILE_NAME = "materials_v2.parquet"
DOWNLOAD_URL = "https://github.com/chenpengyyds/stable-material-project-/releases/download/v2/materials_v2.parquet"

# --- 严格匹配你 CSV 的原始列名 ---
# 这里的列名必须和你 CSV 第一行完全一致
SEARCH_COLS = [
    'Material ID', 
    'Formula', 
    'Predicted Formation Energy (eV/atom)', 
    'Band Gap (eV)', 
    'Space Group', 
    'Total Energy (eV)'
]

df_search = pd.DataFrame()
is_downloading = False

def background_init():
    global df_search, is_downloading
    is_downloading = True
    
    # 1. 下载逻辑
    if not os.path.exists(FILE_NAME):
        print("⏳ [后台] 正在下载数据库...")
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
            print("✅ [后台] 下载完成！")
        except Exception as e:
            print(f"❌ [后台] 下载失败: {e}")
            is_downloading = False
            return

    # 2. 静态加载：严格按 SEARCH_COLS 读取
    if os.path.exists(FILE_NAME):
        try:
            # 只加载物理属性列，不加载 cif_text 节省内存
            df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
            print(f"✅ [后台] 索引就绪！当前列: {df_search.columns.tolist()}")
        except Exception as e:
            print(f"❌ [后台] 加载出错，请检查 Parquet 列名是否包含: {SEARCH_COLS}\n错误信息: {e}")
            
    is_downloading = False

@app.on_event("startup")
async def startup_event():
    threading.Thread(target=background_init).start()

@app.get("/search")
async def search(formula: str = "", energy: float = 0.5, page: int = 1):
    if df_search.empty:
        raise HTTPException(status_code=503, detail="数据库装填中，请稍后刷新")
    
    try:
        # 筛选：使用 CSV 里的形成能列名
        target_col = 'Predicted Formation Energy (eV/atom)'
        mask = (df_search[target_col] <= energy)
        
        if formula:
            mask &= (df_search['Formula'].str.contains(formula, case=False, na=False))
        
        results = df_search[mask]
        
        # 分页
        page_size = 20
        start = (page - 1) * page_size
        # to_dict 会把该行所有列（含总能）发给前端
        data = results.iloc[start:start + page_size].to_dict(orient="records")
        
        return {"total": len(results), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"后端搜索出错: {str(e)}")

@app.get("/get_cif")
async def get_cif(mpid: str):
    if not os.path.exists(FILE_NAME):
        raise HTTPException(status_code=503, detail="文件下载中")
    try:
        # 使用 DuckDB 查询时，带空格的列名必须加双引号
        query = f'SELECT cif_text FROM "{FILE_NAME}" WHERE "Material ID" = \'{mpid}\''
        result = duckdb.query(query).fetchone()
        
        if not result or not result[0]:
            return {"mpid": mpid, "cif": None}
            
        return {"mpid": mpid, "cif": result[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"CIF 读取失败: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
