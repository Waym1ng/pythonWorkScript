
import os
import datetime
import time


class DoFile():
    # 获取某个磁盘路径里所有文件
    def getFiles(self, strDir, isLoop, overDay):
        files = []
        if len(strDir) <= 0 or not os.path.exists(strDir):
            return files
        dirs = os.listdir(strDir)
        for dir in dirs:
            path = os.path.join(strDir, dir)
            if (os.path.isfile(path) and path.find(".log") >= 0):  # 是.log文件
                if (self.compareFileTime(path, -overDay)):
                    files.append(path)
            elif (os.path.isdir(path) and isLoop):  # 是磁盘
                files.extend(self.getFiles(path, isLoop, overDay))
            else:
                continue
        return files

    # 综合处理磁盘文件
    def doFiles(self, clearDirs, isLoop=False, overDay=3):
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ":执行中...")
        for dir in clearDirs:
            files = self.getFiles(dir, isLoop, overDay)
            print("{}查询出{}个文件".format(dir, len(files)))
            self.clearFiles(files)
        print("执行完毕...")

    # 清除文本文件/目录
    def clearFiles(self, files):
        for file in files:
            f = file[:file.find(".log") - 2]
            if f.endswith("00:00"):
                strcmd = "rm -rf {}".format(f)
                self.exec_cmd(strcmd)
            print(f)

    # 执行脚本命令
    def exec_cmd(self, strcmd):
        os.system(strcmd)

    # 获取文件创建时间
    def getCreateFileTime(self, path):
        return os.path.getctime(path)

    # 时间戳转datetime
    def TimeStampToTime(self, timestamp):
        return datetime.datetime.utcfromtimestamp(timestamp)

    # 比较当前时间与文件创建时间差值（天）
    def compareFileTime(self, path, overDay):
        comparTime = self.TimeStampToTime(self.getCreateFileTime(path))
        now = datetime.datetime.utcnow() + datetime.timedelta(days=overDay)
        return now > comparTime


if __name__ == '__main__':

    clearDirs = ["/data/logs/airflow"]
    doFile = DoFile()
    doFile.doFiles(clearDirs, True, 45)