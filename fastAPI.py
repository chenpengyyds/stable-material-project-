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

# ==========================================
# 🚨 核心修复：坚决不瞎编，严格按照日志里的列名加载
# ==========================================
SEARCH_COLS = [
    'Material ID', 
    'Formula', 
    'Predicted Formation Energy (eV/atom)', 
    'Band Gap (eV)', 
    'Space Group', 
    'Energy Above Hull (eV/atom)'  # ⬅️ 换成了你日志里的真实名字
]

df_search = pd.DataFrame()
is_downloading = False

def background_init():
    global df_search, is_downloading
    is_downloading = True
    
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

    if os.path.exists(FILE_NAME):
        try:
            # 现在列名对齐了，这里绝对不会再报 No match for FieldRef 错误了
            df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
            print(f"✅ [后台] 索引就绪！当前列: {df_search.columns.tolist()}")
        except Exception as e:
            print(f"❌ [后台] 加载出错: {e}")
            
    is_downloading = False

@app.on_event("startup")
async def startup_event():
    threading.Thread(target=background_init).start()

@app.get("/search")
async def search(formula: str = "", energy: float = 0.5, page: int = 1):
    if df_search.empty:
        raise HTTPException(status_code=503, detail="数据库装填中，请稍后刷新")
    
    try:
        # 💡 使用 E_hull 作为真正的筛选标准
        target_col = 'Energy Above Hull (eV/atom)'
        mask = (df_search[target_col] <= energy)
        
        if formula:
            mask &= (df_search['Formula'].str.contains(formula, case=False, na=False))
        
        results = df_search[mask]
        
        page_size = 20
        start = (page - 1) * page_size
        data = results.iloc[start:start + page_size].to_dict(orient="records")
        
        return {"total": len(results), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"后端搜索出错: {str(e)}")

@app.get("/get_cif")
async def get_cif(mpid: str):
    if not os.path.exists(FILE_NAME):
        raise HTTPException(status_code=503, detail="文件下载中")
    try:
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
