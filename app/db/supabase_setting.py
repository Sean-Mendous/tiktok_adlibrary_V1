from supabase import create_client, Client

url = "https://mdkkagkodzgbhamgvdql.supabase.co"
key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1ka2thZ2tvZHpnYmhhbWd2ZHFsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDcyMjA2MjksImV4cCI6MjA2Mjc5NjYyOX0.oWNK6bpPFSp-c4D47XN2l4r_zQfv4q6-qxWrkhxVGBU"
supabase: Client = create_client(url, key)
table = "first_pro"

def supabase_insert(data, table=table, client=supabase):
    res = client.table(table).insert(data).execute()
    return res.data

def supabase_select(data, table=table, client=supabase):
    res = client.table(table).select("*").eq(data).execute()
    return res.data


# # Insert
# data = {"name": "田中酢豚郎", "number": 2000, "used": False}
# res = supabase.table("first_pro").insert(data).execute()
# print(res.data)

# # Select
# res = supabase.table("first_pro").select("*").execute()
# for row in res.data:
#     print(f'for ROW {row["id"]} > name: {row["name"]} number: {row["number"]} used: {row["used"]}')

# # Select (Query)
# res = supabase.table("first_pro").select("*").like("name", "田中%").execute()
# for row in res.data:
#     print(f'for ROW {row["id"]} > name: {row["name"]} number: {row["number"]} used: {row["used"]}')

if __name__ == "__main__":
    data = {"name": "とんちんかん", "number": 2000, "used": False}
    res = supabase_insert(data)
    print(res)
