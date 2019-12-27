## 初始化须知
### 环境
1. python 2.7.9
2. mongodb(可以连接外部的)
3. redis（可以连接外部的）

### 运行环境恢复步骤
1. 安装python虚拟环境（避免影响全局的python环境）
```text
pip install virtualenv
# 创建虚拟环境
cd xxxx #定位到项目目录
virtualenv venv
# 激活虚拟环境
cd ./venv/Scripts/
activate
cd ../..
```
2. 安装python依赖，激活虚拟环境后，cd到项目所在目录，运行 
```text
pip install -r requirements.txt
```
3. 如果有引用新的python第三方包，引用后请重新生成reuirements.txt文件
```text
pip freeze > requirements.txt
```

### 打包指令
> pyinstaller安装： `pip install pyinstaller`
```text
pyinstaller boot.spec
```
打包完成后，会将exe生成在项目根目录的dist文件夹中