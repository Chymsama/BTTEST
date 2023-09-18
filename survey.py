#!/usr/bin/env python
# coding: utf-8

# # Khảo sát Năng lực triển khai dự án Phân tích dữ liệu

# Chào các bạn học viên tại **CodeGym**, nhằm tìm hiểu sự hiệu quả trong quá trình giảng dạy, cũng như để biết được các bạn đang gặp khó khăn nào trong quá trình học, GV Tô Thanh Đức đã tạo ra mô phỏng dữ liệu doanh nghiệp để các bạn thực hành.

# Ngữ cảnh: **Một công ty cần tạo 1 *báo cáo* để nắm bắt tình hình hoạt động Nhập/Xuất kho**.

# Dữ liệu đầu vào:![data_model.jpg](attachment:06716441-854b-432b-b8cd-48fd34240050.jpg)

# **warehouse: Bảng Nhập kho**
# - date (int): ngày Nhập kho
# - item_id (int): Mã sản phẩm Nhập kho
# - amount (int): Khối lượng Nhập kho
# - pic_id (int): Mã nhân viên phụ trách Nhập kho cho sản phẩm đó

# **export: Bảng Xuất kho**
# - date (int): ngày Xuất kho
# - item_id (int): Mã sản phẩm Xuất kho
# - amount (int): Khối lượng Xuất kho
# - pic_id (int): Mã nhân viên phụ trách Xuất kho cho sản phẩm đó

# **pic: Danh sách nhân viên**
# - id (int): Mã nhân viên
# - first_name (object): Tên nhân viên
# - last_name (object): Họ nhân viên

# **item: Danh sách mặt hàng**
# - id (int): Mã mặt hàng
# - item_name (object): Tên mặt hàng
# - sub_category_id (int): Mã Loại hàng hóa Level 2

# **sub_category: Danh sách Loại hàng hóa Level 2**
# - id (int): Mã Loại hàng hóa Level 2
# - sub_category (object): Tên Loại hàng hóa Level 2
# - category_id (int): Mã Loại hàng hóa Level 1

# **category: Danh sách Loại hàng hóa Level 1**
# - id (int): Mã Loại hàng hóa Level 1
# - category (object): Tên Loại hàng hóa Level 1

# ## Phần 1. Làm sạch Logic của data (mẫu)

# **Lưu ý:** Học viên chạy toàn bộ code của Phần 1 trước khi làm các nội dung sau.

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime


# ### **Ô này lưu toàn bộ đường dẫn, tên sheet của các bảng trong file Excel**

# In[2]:


data_path = "D:\codegym3\\"
data_name = 'warehouse_tracking (1).xlsx'

warehouse_sh = 'warehouse'
export_sh = 'export'
item_sh = 'item'
sub_category_sh = 'sub_category'
category_sh = 'category'
pic_sh = 'pic'


# **Truy cập vào dữ liệu, tạo các Pandas objects**

# In[3]:


warehouse_df = pd.read_excel(data_path + data_name, sheet_name = warehouse_sh)
warehouse_df.head()


# In[4]:


export_df = pd.read_excel(data_path + data_name, sheet_name = export_sh)
export_df.head()


# In[5]:


warehouse_df['date'] = pd.to_datetime(warehouse_df['date'] - 25569, unit = 'd')
export_df['date'] = pd.to_datetime(export_df['date'] - 25569, unit = 'd')


# In[6]:


export_df.head()


# In[7]:


warehouse_df.head()


# xác định ngày xuất hàng trước ngày nhập kho đầu tiên

# In[8]:


export_df['item_id'].nunique()


# In[9]:


warehouse_df['item_id'].nunique()


# In[10]:


cond = warehouse_df['item_id']==4
warehouse_df.loc[cond, :]['date'].min()


# In[11]:


warehouse_1st=warehouse_df.groupby(['item_id'])['date'].min().reset_index().rename(columns = {'date': '1st_warehouse_day'})


# In[12]:


export_df = pd.merge(export_df,warehouse_1st,how='left',left_on=['item_id'],right_on=['item_id'])


# In[13]:


export_df.head(5)


# In[14]:


export_df['1st_warehouse_day']=export_df['1st_warehouse_day'].replace(np.nan,pd.to_datetime('2023-01-01')) 


# In[15]:


# detect the 1st day in warehouse
wh_1st_day = warehouse_df.groupby(['item_id'])['date'].min().reset_index()
wh_1st_day.head()


# In[16]:


# 1st warehouse_day merge to export
export_df = pd.merge(export_df, wh_1st_day
                     ,how = 'left'
                     ,on = ['item_id']
                     ,suffixes=('_ex', '_wh'))
export_df.head()


# In[17]:


date_valid_export_row_cond = export_df['date_ex'] >= export_df['date_wh']
date_valid_export_col_cond = ['date_ex', 'item_id', 'amount', 'pic']
date_valid_ex = export_df.loc[date_valid_export_row_cond, date_valid_export_col_cond].copy()
date_valid_ex.shape[0]


# In[18]:


date_valid_ex['item_id'].nunique()


# In[19]:


date_valid_ex.head()


# In[20]:


def detect_wrong_records(item_id, warehouse_df, export_df):
    # Lọc bảng Nhập kho từng item_id một
    wh_row_cond = warehouse_df['item_id'] == item_id
    wh_col_cond = ['date', 'item_id', 'amount']
    item_id_wh = warehouse_df.loc[wh_row_cond, wh_col_cond].copy()
    
    # Lọc bảng Xuất kho từng item_id một
    ex_row_cond = export_df['item_id'] == item_id
    ex_col_cond = ['date_ex', 'item_id', 'amount']
    item_id_ex = export_df.loc[ex_row_cond, ex_col_cond].copy()
    
    # Tổng hợp biến động Nhập/Xuất mỗi ngày (giả định đơn vị nhỏ nhất là Ngày)
    wh_by_date = item_id_wh.groupby(['date'])['amount'].sum().reset_index()
    ex_by_date = item_id_ex.groupby(['date_ex'])['amount'].sum().reset_index()
    
    # Tạo 1 bảng với 1 chuỗi ngày, từ Ngày Nhập kho Đầu tiên đến Ngày Xuất kho Cuối cùng
    all_date_ = pd.date_range(wh_by_date['date'].min(), ex_by_date['date_ex'].max()).tolist()
    all_date_df = pd.DataFrame({'all_date':all_date_})
    
    # Ghép số liệu Nhập kho vào bảng all_date
    all_date_df = pd.merge(all_date_df, wh_by_date
                          ,how = 'left'
                          ,left_on = ['all_date']
                          ,right_on = ['date']).loc[:,['all_date', 'amount']].copy()
    
    # Ghép số liệu Xuất kho vào bảng all_date
    all_date_df = pd.merge(all_date_df, ex_by_date
                          ,how = 'left'
                          ,left_on = ['all_date']
                          ,right_on = ['date_ex']
                          ,suffixes=('_wh', '_ex')).loc[:, ['all_date', 'amount_wh', 'amount_ex']].copy()
    
    # Bài toán không yêu cầu tính toán tồn kho "mỗi ngày",
    # nên những ngày không có record cần được loại bỏ
    all_date_df.dropna(subset = ['amount_wh', 'amount_ex'], how = 'all', inplace = True)
    
    # Thay thế tất cả giá trị rỗng bằng sô 0
    all_date_df.fillna(0, inplace = True)
    
    # Reset lại index của bảng all_date sau khi đã loại những ngày không record
    # Nhằm thuận tiện hơn cho for loop
    all_date_df.reset_index(drop = True, inplace = True)
    
    # Tạo 1 list rỗng chứa những ngày không hợp lệ
    invalid_date = []
    
    # Tạo 1 list rỗng chứa những khối lượng không hợp lệ
    invalid_amount = []
    
    # Tạo 1 biến đếm số lượng tồn kho, bắt đầu từ Ngày nhập kho đầu tiên
    stock = all_date_df['amount_wh'][0] - all_date_df['amount_ex'][0]
    
    # Dùng vòng for từ ngày thứ 2 kiểm tra điều kiện,
    # nếu khối lượng xuất kho lớn hơn tổng Tồn kho (stock) -> Invalid
    # khi xác định Invalid, Tồn kho chỉ đếm cộng dồn Nhập kho của ngày đó
    # đồng thời lưu lại giá trị Ngày Xuất kho, Khối lượng xuất không hợp lệ
    # Nếu khối lượng xuất kho nhỏ hơn tổng Tồn kho (stock) -> Valid
    # khi xác định Valid, Tồn kho đếm cộng dồn Nhập kho trừ Xuất kho của ngày đó
    for _ in range(1, all_date_df.shape[0]):
        if all_date_df['amount_ex'][_] > stock:
            stock += all_date_df['amount_wh'][_]
            invalid_date += [all_date_df['all_date'][_]]
            invalid_amount += [all_date_df['amount_ex'][_]]
        else:
            stock += all_date_df['amount_wh'][_] - all_date_df['amount_ex'][_]
    
    # Tạo 1 bảng chứa các records không hợp lệ
    wrong_records_df = pd.DataFrame({'invalid_date':invalid_date
                                     , 'invalid_amount':invalid_amount})
    
    # Xác định giá trị item_id cho bảng này luôn
    wrong_records_df['item_id'] = item_id
    return wrong_records_df


# In[21]:


# Thư viện tqdm dùng để tạo 1 thanh tiến trình
# giúp nhận biết vòng for đang hoạt động
from tqdm import tqdm

# Tạo 1 bảng trống, chứa sẵn các cột sẽ được tạo ra trong bảng wrong_records_df
wrong_records = pd.DataFrame(columns = ['invalid_date', 'invalid_amount', 'item_id'])
for _ in tqdm(date_valid_ex['item_id'].unique().tolist()):
    wrong_records_each_id = detect_wrong_records(item_id = _
                                                 , warehouse_df = warehouse_df
                                                 , export_df = date_valid_ex)
    
    # Dùng pd.concat để nối các bảng có cùng tên cột
    wrong_records = pd.concat([wrong_records, wrong_records_each_id], ignore_index=True)


# In[22]:


wrong_records


# In[23]:


# Dùng pd.to_datetime để chỉnh lại cột 'invalid_date' về dạng datetime
wrong_records['invalid_date'] = pd.to_datetime(wrong_records['invalid_date'])


# In[24]:


# Anti Join
# Ý tưởng từ Left Join, và chọn những dòng có giá trị là NaN ở bảng bên phải
date_amount_valid_ex = pd.merge(date_valid_ex, wrong_records
                         ,how = 'left'
                         ,left_on = ['date_ex','item_id']
                         ,right_on = ['invalid_date', 'item_id'])


# In[25]:


date_amount_valid_ex


# In[26]:


date_amount_valid_ex['invalid_amount'].isnull()


# In[27]:


anti_cond = date_amount_valid_ex['invalid_amount'].isnull()
col_cond = ['date_ex', 'item_id', 'amount', 'pic']
date_amount_valid_ex = date_amount_valid_ex.loc[anti_cond, col_cond].copy().reset_index(drop = True)


# In[28]:


date_amount_valid_ex


# *Và bây giờ ta đã có bảng **Xuất kho** được lưu với tên biến **date_amount_valid_ex** với dữ liệu đã được làm sạch Logic, sẵn sàng cho phân tích.*

# ## Phần 2. Phân tích dữ liệu (20 điểm)

# #### 1. Truy cập vào các bảng **Dimension** và lưu vào Pandas Object *(1đ)*

# In[29]:


warehouse_df = pd.read_excel(data_path + data_name, sheet_name = warehouse_sh)
export_df = pd.read_excel(data_path+ data_name, sheet_name= export_sh)
pic_df = pd.read_excel(data_path+data_name,sheet_name = pic_sh)
item_df = pd.read_excel(data_path+data_name, sheet_name= item_sh)
sub_category_df = pd.read_excel(data_path+data_name,sheet_name= sub_category_sh)
category_df = pd.read_excel(data_path+data_name,sheet_name= category_sh)



# #### 2. Có bảng nào có dữ liệu rỗng (null) không? *(1đ)*

# In[30]:


is_warehouse_null =warehouse_df.isnull().values.any()
is_export_null=export_df.isnull().values.any()
is_pic_null =pic_df.isnull().values.any()
is_item_null =item_df.isnull().values.any()
is_sub_category_null =sub_category_df.isnull().values.any()
is_category_null =category_df.isnull().values.any()
print(f"Bảng Nhập kho có dữ liệu rỗng: {is_warehouse_null}")
print(f"Bảng Xuất kho có dữ liệu rỗng: {is_export_null}")
print(f"Bảng Danh sách nhân viên có dữ liệu rỗng: {is_pic_null}")
print(f"Bảng Danh sách mặt hàng có dữ liệu rỗng: {is_item_null}")
print(f"Bảng Danh sách Loại hàng hóa Level 2 có dữ liệu rỗng: {is_sub_category_null}")
print(f"Bảng Danh sách Loại hàng hóa Level 1 có dữ liệu rỗng: {is_category_null}")


# #### 3. Sản phẩm nào được nhập kho **nhiều lần nhất**? *(1đ)*

# In[31]:


most_imported_product = warehouse_df.groupby('item_id')['amount'].sum().nlargest(1)
product_id = most_imported_product.index[0]
total_imported_amount = most_imported_product.values[0]
print(f"Sản phẩm được nhập kho nhiều lần nhất có mã sản phẩm {product_id} và tổng khối lượng nhập kho là {total_imported_amount}.")


# #### 4. Sản phẩm nào **có khối lượng xuất kho lớn nhất** ? *(1đ)*
# 

# In[32]:


x = export_df.groupby(['item_id'])['amount'].sum().nlargest(1)
x.head(3)
product_id = x.index[0]
total_exported_amount = x.values[0]

print(f"Sản phẩm có khối lượng xuất kho lớn nhất có mã sản phẩm {product_id} và tổng khối lượng xuất kho là {total_exported_amount}.")


# #### 5. Ngày nào phải nhập nhiều hàng nhất? *(1đ)*

# In[33]:


daily_imports = warehouse_df.groupby('date')['amount'].sum()
x = daily_imports.idxmax()
total_imported_amount = daily_imports.max()
print(x ," = " ,total_imported_amount )



# #### 6. Tên đầy đủ của nhân viên Nhập hàng nhiều lần nhất, người đó đã xuất hàng bao nhiêu lần? *(2đ)*

# In[137]:


# Tìm nhân viên nhập hàng nhiều lần nhất
# Tìm thông tin nhân viên đó từ bảng Danh sách nhân viên
# Tạo tên đầy đủ của nhân viên
# Tìm số lần nhân viên đó xuất hàng


most_frequent_importer_id= warehouse_df['pic_id'].value_counts().idxmax()

importer_info = pic_df[pic_df['id'] == most_frequent_importer_id].iloc[0]

importer_name = f"{importer_info['first_name']} {importer_info['last_name']}"

export_count = export_df[export_df['pic'] == most_frequent_importer_id]['pic'].count()
print(f"Nhân viên nhập hàng nhiều lần nhất là: {importer_name}")
print(f"Số lần nhân viên này đã xuất hàng: {export_count}")


# #### 7. Category nào được nhập hàng với khối lượng lớn nhất, trong Category đó sản phẩm nào được xuất đi với khối lượng lớn nhất? *(2đ)*

# In[104]:


merged_df = warehouse_df.merge(item_df, left_on='item_id', right_on='id')
merged_df = merged_data.merge(sub_category_df, left_on='sub_category_id', right_on='id')


# In[106]:


# e chạy cái trên nhiều quá nó ra mớ cột mà lặp lại hehe
del merged_df['id_x']
del merged_df['id_y']
del merged_df['sub_category_y']
del merged_df['id']
del merged_df['category_id_y']


# In[127]:


merged_df.head(5)

sub_category_imports = merged_df.groupby(['category_id_x'])['amount'].sum()
sub_category_with_max_imports = category_imports.idxmax()
category_max = category_df[category_df['id']== sub_category_with_max_imports]

catagory_2_lever = category_max.merge(sub_category_df,how='inner',left_on='id', right_on='category_id')

del catagory_2_lever['id_x']


# In[131]:


catagory_2_lever = catagory_2_lever.rename(columns={'id_y':'sub_category_id'})
catagory_2_lever_item = catagory_2_lever.merge(item_df,how='inner', left_on='sub_category_id',right_on='sub_category_id')


# In[135]:


merged_exports = catagory_2_lever_item.merge(export_df,how='inner', left_on='id', right_on='item_id')


# In[144]:


product_with_max_exports = merged_exports.groupby(['item_name'])['amount'].sum().idxmax()

print('Catagory được nhập với số lượng lớn nhất là : ' ,category_max['category'].iloc[0])
print('Sản phẩm được xuất đi với khối lượng lớn nhất là :', product_with_max_exports)


# #### 8. Trong tháng đầu tiên, Sub-category **Binders** được Nhập kho bởi những nhân viên nào? *(3đ)*

# In[68]:


#tạo biến ngày bắt đầu và kết thúc

start_date = warehouse_df['date'].min()
end_date = start_date.replace(day=31, month=start_date.month)

#tiếp theo e tìm id của Binders trong bảng Sub-category
Binders_id = sub_category_df.loc[sub_category_df['sub_category']=='Binders','id'].values[0]

#sau đó là các item_id với Binders_id đã đucợ tìm trên
item_with_Binders_id = item_df.loc[item_df['sub_category_id']== Binders_id, 'id']

#sau đó tìm ra các sản phẩm và thêm điều kiện ngày 
fillter_product = warehouse_df[(warehouse_df['item_id'].isin(item_with_Binders_id)) & (warehouse_df['date']>= start_date) & (warehouse_df['date']<= end_date)]

#tìm các id của nhân viên và dùng unique để tránh lặp lại 
pic_ids = fillter_product['pic_id'].unique()

#cuối cùng e lấy ra tên 
pic_name = pic_df[pic_df['id'].isin(pic_ids)]


pic_name.head(4)


# #### 9. Vào ngày cuối cùng của tháng 5, sub-category **Tables** còn bao nhiêu hàng trong kho? *(4đ)*

# In[90]:


#tìm id của tables trong bảng sub-category

Tables_id = sub_category_df.loc[sub_category_df['sub_category']=='Tables','id'].values[0]

# e nghĩ là vì đã clear các lỗi nhập data ở trên rồi nên bây giờ e nghĩ chỉ cần cộng hết đã nhập và trừ cho xuất kho là oke(em nghĩ vậy)

item_with_Tables_id = item_df.loc[item_df['sub_category_id']== Tables_id, 'id']

#tạo biến end_date và tính tổng sống lượng nhập băng item_id đã tìm được 

end_of_month_date = pd.to_datetime('2022-05-31', format='%Y-%m-%d')
filtered_warehouse = warehouse_df[(warehouse_df['item_id'].isin(item_with_Tables_id)) & (warehouse_df['date'] <= end_of_month_date)]

total_amount_imported = filtered_warehouse['amount'].sum()

filtered_export = export_df[(export_df['item_id'].isin(item_with_Tables_id)) & (export_df['date']<= end_of_month_date)]

total_amount_issued = filtered_export['amount'].sum()
print('Số hanngf Sub-category Tables còn trong ngày cuối cùng tháng 5 là : ', total_amount_imported - total_amount_issued)

# sau khi in ra kết quả thì ehhehhe ... speechless


# 
# #### 10. Những sản phẩm nào **chưa từng được xuất kho**? *(4đ)*

# In[96]:


# Lấy danh sách các item_id
all_item_ids  = item_df['id'].unique()

# Lấy danh sách các item_id từ  export_df
exported_item_ids = export_df['item_id'].unique()

# Tìm những item_id chưa từng được xuất kho bằng so sánh 2 danh sách

unexported_item_ids = set(all_item_ids)- set(exported_item_ids)

#Lâys thongo tin item
unexported_items = item_df[item_df['id'].isin(unexported_item_ids)]

unexported_items.head()

