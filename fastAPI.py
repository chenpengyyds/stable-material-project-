from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import urllib.request

app = FastAPI()

# 允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 配置区 ---
FILE_NAME = "materials_v2.parquet"
# 这是你图片中 Release 文件的直接下载链接
DOWNLOAD_URL = "https://github.com/chenpengyyds/stable-material-project-/releases/download/v2/materials_v2.parquet"
# 搜索时加载的轻量化列
SEARCH_COLS = ['Material ID', 'Formula', 'Predicted Formation Energy (eV/atom)', 'Band Gap (eV)', 'Space Group']

df_search = pd.DataFrame()

def download_database():
    """如果磁盘没有数据库，则从 GitHub 下载"""
    if not os.path.exists(FILE_NAME):
        print(f"⏳ 正在从 GitHub 搬运数据库...")
        try:
            opener = urllib.request.build_opener()
            opener.addheaders = [('User-Agent', 'Mozilla/5.0')]
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
            print("✅ 数据库下载成功！")
        except Exception as e:
            print(f"❌ 下载失败: {e}")

@app.on_event("startup")
async def startup_event():
    """服务器启动时：下载并加载索引"""
    global df_search
    download_database()
    if os.path.exists(FILE_NAME):
        try:
            # 只读取搜索列，节省 Railway 内存
            df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
            print(f"✅ 索引加载成功：{len(df_search)} 条材料")
        except Exception as e:
            print(f"❌ 读取索引出错: {e}")

@app.get("/search")
async def search(formula: str = "", energy: float = 0.5):
    if df_search.empty:
        raise HTTPException(status_code=503, detail="数据库初始化中")
    
    # 筛选逻辑
    mask = (df_search['Predicted Formation Energy (eV/atom)'] <= energy)
    if formula:
        mask &= (df_search['Formula'] == formula)
    
    results = df_search[mask].head(100) # 限制返回数量提升速度
    return results.to_dict(orient="records")

@app.get("/get_cif")
async def get_cif(mpid: str):
    print(f"🔍 正在检索 ID: {mpid} 的结构文件...")
    try:
        # 精准读取：只读取该 ID 的 cif_text 列
        # 🚨 注意：如果你的 Parquet 里列名不是 cif_text，请修改下面两处的名称
        single_df = pd.read_parquet(
            FILE_NAME, 
            filters=[('Material ID', '==', mpid)],
            columns=['Material ID', 'cif_text'] 
        )
        
        if single_df.empty:
            print(f"❌ 找不到 ID: {mpid}")
            raise HTTPException(status_code=404, detail="未找到该材料")
            
        cif_content = single_df.iloc[0].get('cif_text', "")
        
        if not cif_content or str(cif_content) == "nan":
            print(f"⚠️ {mpid} 数据存在，但 CIF 内容为空")
            return {"mpid": mpid, "cif": None}
            
        return {"mpid": mpid, "cif": cif_content}
    except Exception as e:
        print(f"🔥 后端读取异常: {str(e)}")
        raise HTTPException(status_code=500, detail=f"后端报错: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
