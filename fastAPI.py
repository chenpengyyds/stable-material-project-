
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import urllib.request # 导入下载工具

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FILE_NAME = "materials_v2.parquet"
# 🚨 厂长注意：把下面这个网址，换成你刚才右键复制的真实下载链接！
DOWNLOAD_URL = "https://github.com/填你自己的/releases/download/v2.0/materials_v2.parquet"

# 🚀 超级自动装载机：如果服务器上没有这个文件，就自动去云端拉取
if not os.path.exists(FILE_NAME):
    print(f"⏳ 发现缺失数据库，正在从 GitHub Release 高速拉取 172MB 超大文件...")
    urllib.request.urlretrieve(DOWNLOAD_URL, FILE_NAME)
    print("✅ 超大数据库下载完成！")

SEARCH_COLS = ['Material ID', 'Formula', 'Predicted Formation Energy (eV/atom)', 'Band Gap (eV)', 'Space Group']

try:
    df_search = pd.read_parquet(FILE_NAME, columns=SEARCH_COLS)
    print(f"✅ 搜索索引加载成功：{len(df_search)} 条材料")
except Exception as e:
    print(f"❌ 加载失败: {e}")
@app.get("/search")
async def search(formula: str = "", energy: float = 0.5, page: int = 1):
    results = df_search[df_search['Predicted Formation Energy (eV/atom)'] <= energy]
    if formula:
        results = results[results['Formula'].str.contains(formula, case=False, na=False)]
    
    # 🚀 响应厂长要求：每页固定返回 10 条数据
    page_size = 10 
    start = (page - 1) * page_size
    
    valid_cols = [c for c in SEARCH_COLS if c in results.columns]
    data = results.iloc[start:start + page_size][valid_cols].to_dict(orient="records")
    
    return {"total": len(results), "data": data}

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
