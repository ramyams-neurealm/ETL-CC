# ETL CC

Simple NeuFlow ETL Migration Command Center project structure.

Current implementation focus:
1. Test direct Informatica repository connection
2. Retrieve mapping list
3. Select entire repository or selected mappings
4. Encrypt and save repository credentials
5. Retrieve and save full mapping metadata

Run:
```bash
pip install -r requirements.txt
uvicorn etl_cc.main:app --app-dir src --reload
```
