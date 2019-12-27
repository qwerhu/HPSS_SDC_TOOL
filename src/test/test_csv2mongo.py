import time
from workers import hpss_csv_to_mongo

print('开始导入')
t = time.time()
hpss_csv_to_mongo.main(r'G:\HugeGIS_WS\Z_智慧水厂\DOC\jzt_scada.csv')
print('cost time: ' + str(time.time() - t))
print('导入结束')
