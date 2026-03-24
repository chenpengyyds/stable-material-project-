from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import urllib.request
import threading

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FILE_NAME = "materials_v2.parquet"
DOWNLOAD_URL = "https://github.com/chenpengyyds/stable-material-project-/releases/download/v2/materials_v2.parquet"
SEARCH_COLS = ['Material ID', 'Formula', 'Predicted Formation Energy (eV/atom)', 'Band Gap (eV)', 'Space Group']

df_search = pd.DataFrame()
is_downloading = False  # 记录后台是否正在干活

def background_init():
    """后台独立运行的任务，绝不阻塞服务器启动"""
    global df_search, is_downloading
    is_downloading = True
    
    if not os.path.exists(FILE_NAME):
        print("⏳ [后台任务] 正在从 GitHub 下载 169MB 数据库，请稍候...")
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
            print("✅ [后台任务] 数据库下载成功！")
        except Exception as e:
            print(f"❌ [后台任务] 下载失败: {e}")
            is_downloading = False
            return

    if os.path.exists(FILE_NAME):
        try:
            print("⏳ [后台任务] 正在将数据加载到内存...")
            df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
            print(f"✅ [后台任务] 索引加载完成：共 {len(df_search)} 条材料！")
        except Exception as e:
            print(f"❌ [后台任务] 读取数据出错: {e}")
            
    is_downloading = False

@app.on_event("startup")
async def startup_event():
    # 核心修复点：将下载任务扔到后台线程去执行，FastAPI 0.1秒瞬间启动完
    threading.Thread(target=background_init).start()

@app.get("/search")
async def search(formula: str = "", energy: float = 0.5):
    # 如果用户在刚启动的几秒内搜索，给出优雅提示
    if df_search.empty:
        if is_downloading:
            raise HTTPException(status_code=503, detail="云端正在后台下载数据库，请等待约 1 分钟后再试")
        else:
            raise HTTPException(status_code=503, detail="数据库未就绪")
    
    mask = (df_search['Predicted Formation Energy (eV/atom)'] <= energy)
    if formula:
        mask &= (df_search['Formula'] == formula)
    
    results = df_search[mask].head(100)
    return results.to_dict(orient="records")

@app.get("/get_cif")
async def get_cif(mpid: str):
    if not os.path.exists(FILE_NAME):
        raise HTTPException(status_code=503, detail="云端数据库尚未下载完成，请稍后再试")
        
    try:
        single_df = pd.read_parquet(
            FILE_NAME, 
            filters=[('Material ID', '==', mpid)],
            columns=['Material ID', 'cif_text'] 
        )
        
        if single_df.empty:
            raise HTTPException(status_code=404, detail="未找到该材料")
            
        cif_content = single_df.iloc[0].get('cif_text', "")
        
        if not cif_content or str(cif_content) == "nan":
            return {"mpid": mpid, "cif": None}
            
        return {"mpid": mpid, "cif": cif_content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
