# -*- coding: utf-8 -*-
import pandas as pd
import os
from  datetime import datetime,timedelta



def modify_excel_format(excel_data, writer, df):
    # ----------调整excel格式 ---------------
    workbook = writer.book
    fmt = workbook.add_format({"font_name": u"宋体"})
    col_fmt = workbook.add_format(
        {'bold': True, 'font_size': 11, 'font_name': u'宋体', 'border': 1, 'bg_color': '#0265CB','font_color': 'white',
         'valign': 'vcenter', 'align': 'center'})
    detail_fmt = workbook.add_format(
        {"font_name": u"宋体", 'border': 1, 'valign': 'vcenter', 'align': 'center','font_size': 11, 'text_wrap': True})
    worksheet1 = writer.sheets['Sheet1']
    for col_num, value in enumerate(df.columns.values):
        worksheet1.write(0, col_num, value, col_fmt)
    # # 设置列宽行宽
    worksheet1.set_column('A:F', 20, fmt)
    worksheet1.set_row(0, 30, fmt)
    for i in range(1, len(excel_data)+1):
        worksheet1.set_row(i, 27, detail_fmt)

    return True



excel_data = []
columns = [
    "用户", "名字", "标题", "类", "测试", "python"
]
tmp = ["张同学", "张同学", "pd 生成excel 调整格式的demo", "pandas", "pd", "python"]
excel_data.append(tmp)
df = pd.DataFrame(data=excel_data, columns=columns)
t = datetime.now().date() - timedelta(days=1)
with pd.ExcelWriter(path='样式%d%02d%02d.xlsx' % (t.year, t.month, t.day), engine="xlsxwriter") as writer:
    df.to_excel(writer, sheet_name='Sheet1', encoding='utf8', header=False, index=False, startcol=0, startrow=1)
    modify_excel_format(excel_data, writer, df)
    writer.save()
    print('ok!')
