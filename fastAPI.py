from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import urllib.request

app = FastAPI()

# 解决跨域问题，让前端能访问后端
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FILE_NAME = "materials_v2.parquet"
# 数据库下载地址
DOWNLOAD_URL = "https://github.com/chenpengyyds/stable-material-project-/releases/download/v2/materials_v2.parquet"

# 定义需要加载的列（索引）
SEARCH_COLS = ['Material ID', 'Formula', 'Predicted Formation Energy (eV/atom)', 'Band Gap (eV)', 'Space Group']
df_search = pd.DataFrame() # 初始化一个空表防止报错

def download_database():
    if not os.path.exists(FILE_NAME):
        print(f"⏳ 正在下载数据库...")
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
            print("✅ 数据库下载成功！")
        except Exception as e:
            print(f"❌ 下载失败: {e}")

@app.on_event("startup")
async def startup_event():
    global df_search
    download_database()
    if os.path.exists(FILE_NAME):
        try:
            # 只读取索引列，节省 Railway 内存
            df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
            print(f"✅ 索引加载成功：{len(df_search)} 条材料")
        except Exception as e:
            print(f"❌ 读取索引出错: {e}")

# --- 关键：补全搜索路由 ---
@app.get("/search")
async def search_materials(formula: str, energy: float):
    if df_search.empty:
        raise HTTPException(status_code=503, detail="数据库尚未加载完成，请稍后")
    
    try:
        # 筛选逻辑：化学式匹配 + 形成能阈值
        mask = (df_search['Formula'] == formula) & (df_search['Predicted Formation Energy (eV/atom)'] <= energy)
        results = df_search[mask]
        return results.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/get_cif")
async def get_cif(mpid: str):
    try:
        # 仅在需要时精准读取 CIF 数据，防止内存溢出
        single_df = pd.read_parquet(FILE_NAME, filters=[('Material ID', '==', mpid)])
        if single_df.empty:
            raise HTTPException(status_code=404, detail="未找到该材料")
        
        cif_text = single_df.iloc[0].get('cif_text', "")
        return {"mpid": mpid, "cif": cif_text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
