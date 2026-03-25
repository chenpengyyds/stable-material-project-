from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import urllib.request
import threading
import duckdb

app = FastAPI()

# 解决跨域，确保 GitHub Pages 能访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FILE_NAME = "materials_v2.parquet"
DOWNLOAD_URL = "https://github.com/chenpengyyds/stable-material-project-/releases/download/v2/materials_v2.parquet"

# 动态变量
df_search = pd.DataFrame()
SEARCH_COLS = []
is_downloading = False

def background_init():
    """后台任务：搬运数据库并自动分析表头"""
    global df_search, SEARCH_COLS, is_downloading
    is_downloading = True
    
    # 1. 如果没文件就下载
    if not os.path.exists(FILE_NAME):
        print("⏳ [后台] 正在从 GitHub 下载数据库...")
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
            print("✅ [后台] 下载成功！")
        except Exception as e:
            print(f"❌ [后台] 下载失败: {e}")
            is_downloading = False
            return

    # 2. 自动探测所有列名（实现一劳永逸的关键）
    if os.path.exists(FILE_NAME):
        try:
            print("⏳ [后台] 正在分析数据库结构...")
            # 仅读取表头
            sample_df = pd.read_parquet(FILE_NAME, engine='pyarrow').head(1)
            # 排除掉 cif_text 这种非展示列
            SEARCH_COLS = [c for c in sample_df.columns if c != 'cif_text']
            
            # 只加载物理属性列进内存，极度节省空间
            df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
            print(f"✅ [后台] 索引就绪！包含属性: {', '.join(SEARCH_COLS)}")
        except Exception as e:
            print(f"❌ [后台] 加载出错: {e}")
            
    is_downloading = False

@app.on_event("startup")
async def startup_event():
    # 瞬间启动，任务丢给后台线程
    threading.Thread(target=background_init).start()

@app.get("/search")
async def search(formula: str = "", energy: float = 0.5, page: int = 1):
    if df_search.empty:
        status = "正在下载中" if is_downloading else "未就绪"
        raise HTTPException(status_code=503, detail=f"数据库{status}，请稍等")
    
    # 筛选逻辑：支持化学式模糊匹配
    mask = (df_search['Predicted Formation Energy (eV/atom)'] <= energy)
    if formula:
        mask &= (df_search['Formula'].str.contains(formula, case=False, na=False))
    
    results = df_search[mask]
    
    # 分页逻辑
    page_size = 20
    start = (page - 1) * page_size
    data = results.iloc[start:start + page_size].to_dict(orient="records")
    
    return {"total": len(results), "data": data}

@app.get("/get_cif")
async def get_cif(mpid: str):
    """使用 DuckDB 精准切片，彻底解决内存溢出导致的 502"""
    if not os.path.exists(FILE_NAME):
        raise HTTPException(status_code=503, detail="数据库未就位")
        
    try:
        # 直接在磁盘上查，不占内存
        query = f"SELECT cif_text FROM '{FILE_NAME}' WHERE \"Material ID\" = '{mpid}'"
        result = duckdb.query(query).fetchone()
        
        if not result or not result[0]:
            return {"mpid": mpid, "cif": None}
            
        return {"mpid": mpid, "cif": result[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
