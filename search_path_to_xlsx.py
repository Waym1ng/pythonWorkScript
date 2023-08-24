# -*- coding: utf-8 -*-
import pandas as pd

import os
import datetime


class DoFile():
    # 获取某个磁盘路径里所有文件
    def getFiles(self, strDir, isLoop):
        files = []
        if len(strDir) <= 0 or not os.path.exists(strDir):
            return files
        dirs = os.listdir(strDir)
        for dir in dirs:
            path = os.path.join(strDir, dir)
            if (os.path.isfile(path) and path.find(".jpg") >= 0 or path.find(".png") >= 0):
                files.append(path)
            elif (os.path.isdir(path) and isLoop):  # 是磁盘
                files.extend(self.getFiles(path, isLoop))
            else:
                continue
        return files

    # 综合处理磁盘文件
    def doFiles(self, searchDirs, isLoop=False):
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ":执行中...")
        for dir in searchDirs:
            files = self.getFiles(dir, isLoop)
            print("{}查询出{}个文件".format(dir, len(files)))
            self.workingFiles(files)
        print("执行完毕...")

    # 处理文本文件/目录
    def workingFiles(self, files):
        excel_data = []
        for file in files:
            excel_data.append([file, os.path.dirname(file)])
            print(file)

        columns = ["地址", "上级目录"]
        df = pd.DataFrame(data=excel_data, columns=columns)
        t = datetime.datetime.now()
        with pd.ExcelWriter(path='demo-%d%02d%02d%02d%02d%02d.xlsx' % (t.year, t.month, t.day, t.hour, t.minute, t.second), engine="xlsxwriter") as writer:
            df.to_excel(writer, sheet_name='Sheet1', encoding='utf8', header=False, index=True, startcol=0, startrow=1)
            writer.save()
            print('write finish!')
            



if __name__ == '__main__':
    # 绝对路径 
    searchDirs = [input("请输入搜索路径：")]
    doFile = DoFile()
    doFile.doFiles(searchDirs, True)
