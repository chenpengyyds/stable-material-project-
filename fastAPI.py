from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import urllib.request

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FILE_NAME = "materials_v2.parquet"

# 🚀 厂长，我已经根据你的截图帮你把绝对正确的下载链接写好了！
DOWNLOAD_URL = "https://github.com/chenpengyyds/stable-material-project-/releases/download/v2/materials_v2.parquet"

# 🚀 超级自动装载机：每次 Railway 启动时，如果没发现文件，就自动去你的 Release 拉取这 164MB 的数据
if not os.path.exists(FILE_NAME):
    print("⏳ 发现缺失数据库，正在从 GitHub Release 高速拉取 164MB 超大文件，请耐心等待...")
    try:
        urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
        print("✅ 超大数据库下载完成！")
    except Exception as e:
        print(f"❌ 下载失败: {e}")

SEARCH_COLS = ['Material ID', 'Formula', 'Predicted Formation Energy (eV/atom)', 'Band Gap (eV)', 'Space Group']

try:
    # 只读取必要的列，让 Railway 喘口气
    df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
    print(f"✅ 搜索索引加载成功：{len(df_search)} 条材料")
except Exception as e:
    print(f"❌ 加载失败: {e}")

# ==========================================
# 下面保留你原来的 @app.get("/search") 和 @app.get("/get_cif") 代码不变！
# ==========================================

@app.get("/get_cif")
async def get_cif(mpid: str):
    try:
        single_df = pd.read_parquet(FILE_NAME, filters=[('Material ID', '==', mpid)])
        
        if single_df.empty:
            raise HTTPException(status_code=404, detail="未找到该材料")
            
        cif_text = single_df.iloc[0].get('cif_text', "")
        
        # 即使找到了记录，如果 CIF 是空的，也报错拦截
        if not cif_text or str(cif_text).strip() == "":
            raise HTTPException(status_code=404, detail="该材料没有 CIF 结构数据")
            
        return {"mpid": mpid, "cif": cif_text}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
