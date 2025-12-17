Year="2025"
list=`date "+%m%d"`
#rebuild stock

echo $Year$i
mkdir -p ./datatool/vipdoc
cd ./datatool/vipdoc
wget https://data.tdx.com.cn/vipdoc/hsjday.zip && unzip -q hsjday.zip -d . && rm -rf hsjday.zip
cd ../..
./tdx2db init --dayfiledir ./datatool/vipdoc --dbpath tdx.db
./tdx2db cron --dbpath tdx.db --maxday $Year$i

