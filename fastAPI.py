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
# 🚨 厂长，请把下面引号里的网址换成你刚才【右键复制】到的那个真实链接
DOWNLOAD_URL = "https://github.com/chenpengyyds/stabel-material-project/releases/download/v1.0/materials_v2.parquet"

if not os.path.exists(FILE_NAME):
    print(f"⏳ 正在尝试下载数据库，目标地址: {DOWNLOAD_URL}")
    try:
        # 💡 核心升级：把自己伪装成浏览器（Chrome），防止被 GitHub 拦截
        opener = urllib.request.build_opener()
        opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')]
        urllib.request.install_opener(opener)
        
        urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
        print("✅ 超大数据库搬运成功！")
    except Exception as e:
        # 如果失败，这里会打印出非常详细的原因
        print(f"❌ 搬运失败，具体原因: {e}")
        # 即使失败也尝试往下走，看看是不是文件其实已经存在了
        if not os.path.exists(FILE_NAME):
             print("🚨 警告：磁盘上仍然没有发现数据库文件！")

SEARCH_COLS = ['Material ID', 'Formula', 'Predicted Formation Energy (eV/atom)', 'Band Gap (eV)', 'Space Group']

try:
    if os.path.exists(FILE_NAME):
        df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
        print(f"✅ 索引加载成功：{len(df_search)} 条材料")
    else:
        print("❌ 加载中止：数据库文件未就位。")
except Exception as e:
    print(f"❌ 读取 Parquet 出错: {e}")

# ... 后面你的 /search 和 /get_cif 代码保持不变 ...
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
