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
is_downloading = False

def background_init():
    """后台静默加载任务"""
    global df_search, is_downloading
    is_downloading = True
    
    if not os.path.exists(FILE_NAME):
        print("⏳ [后台任务] 正在从 GitHub 下载数据库...")
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
    threading.Thread(target=background_init).start()

# ==========================================
# 🚨 关键修复：恢复了你原本的分页逻辑和返回格式
# ==========================================
@app.get("/search")
async def search(formula: str = "", energy: float = 0.5, page: int = 1):
    if df_search.empty:
        if is_downloading:
            raise HTTPException(status_code=503, detail="后台正在下载数据库，请等待约 1 分钟后再试")
        else:
            raise HTTPException(status_code=503, detail="数据库未就绪")
    
    # 使用你原本的筛选逻辑（支持模糊匹配）
    results = df_search[df_search['Predicted Formation Energy (eV/atom)'] <= energy]
    if formula:
        results = results[results['Formula'].str.contains(formula, case=False, na=False)]
    
    # 恢复你原本的分页计算逻辑
    page_size = 30
    start = (page - 1) * page_size
    data = results.iloc[start:start + page_size].to_dict(orient="records")
    
    # 完美契合前端期望的返回格式！
    return {"total": len(results), "data": data}

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
