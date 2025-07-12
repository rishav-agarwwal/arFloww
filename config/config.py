import psycopg2, json

with open("D:\\Downloads\\AR Flow\\config\\dbconfig.json") as f:
    cfg = json.load(f)

try:
    conn = psycopg2.connect(**cfg)
    print('✅ Connected!')
    conn.close()
except Exception as e:
    print('❌', e)
