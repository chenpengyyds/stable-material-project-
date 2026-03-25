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

df_search = pd.DataFrame()
SEARCH_COLS = []
ENERGY_COL = "" # 自动探测到的能量列名
is_downloading = False

def background_init():
    global df_search, SEARCH_COLS, ENERGY_COL, is_downloading
    is_downloading = True
    
    if not os.path.exists(FILE_NAME):
        print("⏳ [后台] 正在下载数据库...")
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
        except Exception as e:
            print(f"❌ 下载失败: {e}")
            is_downloading = False
            return

    if os.path.exists(FILE_NAME):
        try:
            # 1. 读取表头并自动探测列名
            sample = pd.read_parquet(FILE_NAME, engine='pyarrow').head(1)
            all_cols = sample.columns.tolist()
            
            # 💡 自动寻找包含 "Energy" 且包含 "Formation" 的列作为筛选标准
            for col in all_cols:
                if "Energy" in col and "Formation" in col:
                    ENERGY_COL = col
                    break
            
            # 如果没找到，就默认选第一个包含 Energy 的列
            if not ENERGY_COL:
                ENERGY_COL = next((c for c in all_cols if "Energy" in c), all_cols[2])

            SEARCH_COLS = [c for c in all_cols if c != 'cif_text']
            df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
            print(f"✅ 加载成功！识别到能量筛选列为: [{ENERGY_COL}]")
        except Exception as e:
            print(f"❌ 加载出错: {e}")
            
    is_downloading = False

@app.on_event("startup")
async def startup_event():
    threading.Thread(target=background_init).start()

@app.get("/search")
async def search(formula: str = "", energy: float = 0.5, page: int = 1):
    if df_search.empty:
        raise HTTPException(status_code=503, detail="数据库装填中，请稍后刷新")
    
    # 使用自动探测到的 ENERGY_COL 进行筛选
    try:
        mask = (df_search[ENERGY_COL] <= energy)
        if formula:
            mask &= (df_search['Formula'].str.contains(formula, case=False, na=False))
        
        results = df_search[mask]
        page_size = 20
        start = (page - 1) * page_size
        data = results.iloc[start:start + page_size].to_dict(orient="records")
        return {"total": len(results), "data": data}
    except Exception as e:
        # 如果还是报错，打印出到底是在哪一列崩了
        raise HTTPException(status_code=500, detail=f"筛选出错，请检查列名 {ENERGY_COL}: {str(e)}")

@app.get("/get_cif")
async def get_cif(mpid: str):
    try:
        query = f"SELECT cif_text FROM '{FILE_NAME}' WHERE \"Material ID\" = '{mpid}'"
        result = duckdb.query(query).fetchone()
        if not result or not result[0]: return {"mpid": mpid, "cif": None}
        return {"mpid": mpid, "cif": result[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
