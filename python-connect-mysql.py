import json
import pymysql


class OptionMysql(object):
    def __init__(self, options):
        host = options['HOST']
        user = options['USERNAME']
        password = options['PASSWORD']
        database = options['DATABASE']
        port = options['PORT']
        charset = 'utf8'
        # 连接数据库
        self.conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database, charset=charset)
        # 创建游标
        self.cur = self.conn.cursor()
        self.dict_cur = self.conn.cursor(cursor=pymysql.cursors.DictCursor)

    def __del__(self):
        self.cur.close()
        self.dict_cur.close()
        self.conn.close()

    def insert_data(self, sql, params=[]):
        """插入行"""
        try:
            if not params:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql, params)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

        return True

    def get_data_dict(self, sql, params=[]):
        """查询，返回字典类型"""
        try:
            if params:
                self.dict_cur.execute(sql, params)
            else:
                self.dict_cur.execute(sql)
            data = self.dict_cur.fetchall()
            return data
        except Exception as e:
            self.conn.rollback()
            raise e

    def get_data(self, sql, params=[]):
        """查询"""
        self.cur.execute(sql, params)
        data = self.cur.fetchall()
        return data

    def update_data(self, sql, params=[]):
        """更新"""
        try:
            if not params:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql, params)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
        return True

    def delete_data(self, sql, params=[]):
        """删除"""
        try:
            self.cur.execute(sql, params)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            raise e

    def findOne(self, table, col=[], params=[]):
        """
        验证记录是否存在
        Args:
          table: string, 数据表
          col:   list, 查询的列名
          params:list,参数列表
        """
        sql = "SELECT " + ",".join(col) + " FROM " + table + " WHERE deleted_at = '0000-01-01 00:00:00'"
        # WHERE条件
        for i in range(len(col)):
            col[i] = col[i] + " = %s"
        sql += " AND " + ",".join(col)

        self.cur.execute(sql, params)
        data = self.cur.fetchall()
        return data

    def update(self, table, where, args: object):
        """
        更新数据
        Args:
          table: string, 表名
          where:  string, 待更新的row 的where条件
          args:  object, 更新的数据
        """
        column = []
        values = []
        # 提取更新的列及值
        for key in args.keys():
            column.append("`" + key + "`" + " = %s")
            if isinstance(args[key], dict):
                args[key] = str(json.dumps(args[key]))
            values.append(args[key])
        # update sql
        sql = "UPDATE " + table + " SET " + ",".join(column) + " WHERE " + where
        try:
            self.cur.execute(sql, values)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            raise e
