当前目录:/datatool/

参数错误!

通达信深沪行情数据处理工具V4.00.1 用法如下：

配置（文件datatool.ini）范例：

[PATH]

VIPDOC=/tdx/data/vipdoc/

处理指令（注意大小写）：

转档指定日期日线：        ./datatool day  create [开始日期] [终止日期]

删除指定日期日线：        ./datatool day  del    [开始日期] [终止日期]

检查全部日线数据：        ./datatool day	check all

转档指定日期分笔数据：    ./datatool tick create [开始日期] [终止日期]

删除指定日期分笔数据：    ./datatool tick del    [开始日期] [终止日期]

检查全部分笔数据：        ./datatool tick	check all

指定日期分笔数据转分钟：  ./datatool min  create [开始日期] [终止日期]

全部分笔数据转分钟数据：  ./datatool min  create all

删除指定日期分钟数据：    ./datatool min  del    [开始日期] [终止日期]

如果指令中[开始日期]和[终止日期]为同一天，可以只填写一个日期。

例如：./datatool day  create 20161212 和./datatool day  create 20161212 20161212相同

日线和分笔转档，需先手动下载解压文件到vipdoc下的对应目录，日线存入refmhq，分笔存入newdatetick

分钟转档，需要vipdoc下有完整的分笔数据，如果没有则需要先转档分笔

V4.00只支持对2022.5.1之后V4的数据处理,如果行情后端未开启TCKV4,数据处理工具配置中应增加[CTRL] TCKV4=0 配置项


## 工作日
mkdir -p ./datatool/vipdoc/exceptday
cd ./datatool/vipdoc/exceptday
wget https://www.tdx.com.cn/products/autoup/Except2025.zip && unzip Except2025.zip && rm -rf Except2025.zip
cd ../../..
./tdx2db workday  --dbpath tdx.db --wdyear 2025 --wdpath ./datatool/vipdoc/exceptday 

cd ./datatool/vipdoc/exceptday
wget https://www.tdx.com.cn/products/autoup/Except2024.zip && unzip Except2024.zip && rm -rf Except2024.zip
cd ../../..
./tdx2db workday  --dbpath tdx.db --wdyear 2024 --wdpath ./datatool/vipdoc/exceptday 

## 下载日线
mkdir -p ./datatool/vipdoc/refmhq
cd ./datatool/vipdoc/refmhq && wget https://www.tdx.com.cn/products/data/data/g4day/20251201.zip && unzip 20251201.zip && rm -rf 20251201.zip
cd ../../..
docker run -it -v /Users/huguanrui/go/src/github.com/tdx/tdx2db/datatool:/datatool -w /datatool alpine:3.19 ./datatool day create 20251201

## 下载分时
mkdir -p ./datatool/vipdoc/newdatetick
cd ./datatool/vipdoc/newdatetick && wget https://www.tdx.com.cn/products/data/data/g4tic/20251201.zip && unzip 20251201.zip && rm -rf 20251201.zip
cd ../../..
docker run -it -v /Users/huguanrui/go/src/github.com/tdx/tdx2db/datatool:/datatool -w /datatool alpine:3.19 ./datatool tick create 20251201
docker run -it -v /Users/huguanrui/go/src/github.com/tdx/tdx2db/datatool:/datatool -w /datatool alpine:3.19 ./datatool min create 20251201


## 股票例外日更新
./tdx2db workday --wdyear 2024 --wdpath datatool/vipdoc/exceptday --dbpath tdx.db
./tdx2db workday --wdyear 2025 --wdpath datatool/vipdoc/exceptday --dbpath tdx.db

./tdx2db cw --cwpath datatool/vipdoc/tdxfin --cwdl false --dbpath tdx.db

./tdx2db gp --gppath datatool/vipdoc/tdxgp --gpdl false --dbpath tdx.db

股票数据包
mkdir -p ./datatool/vipdoc/tdxgp
cd ./datatool/vipdoc/tdxgp && wget https://data.tdx.com.cn/vipdoc/tdxgp.zip && unzip tdxgp.zip && rm -rf tdxgp.zip


财务数据包
mkdir -p ./datatool/vipdoc/tdxfin
cd ./datatool/vipdoc/tdxfin && wget https://data.tdx.com.cn/vipdoc/tdxfin.zip && unzip tdxfin.zip && rm -rf tdxfin.zip

base数据，包括板块信息
./tdx2db base --basepath /Users/bernard/go/src/tdx/tdx2db/datatool/vipdoc/base --dbpath tdx.db 

