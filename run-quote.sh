Year="2025"
list=`date "+%m%d"`



work="/Volumes/WD"
#work="."

rm -rf ./calc.log

CUR=`pwd`
SYSTEM=`uname  -s`
for i in $list;  
do
if [ $SYSTEM = "Linux" ] ; then
./calc --c conf/config.yml daysplit --in /mnt/e/data/day/$Year/ --out /mnt/e/data/split/ --day $Year$i
elif [ $SYSTEM = "Darwin" ] ; then
mkdir -p ./datatool/vipdoc/refmhq
cd ./datatool/vipdoc/refmhq && wget https://www.tdx.com.cn/products/data/data/g4day/$Year$i.zip && unzip $Year$i.zip && rm -rf $Year$i.zip
cd ../../..
docker run -it -v $CUR/datatool:/datatool -w /datatool alpine:3.19 ./datatool day create $Year$i
rm -rf ./datatool/vipdoc/refmhq/*


./tdx2db cw --cwpath datatool/vipdoc/tdxfin --cwdl false --dbpath tdx.db
./tdx2db base --basepath datatool/vipdoc/base --dbpath tdx.db 
./tdx2db gp --gppath datatool/vipdoc/tdxgp --gpdl false --dbpath tdx.db 
else
    echo  $SYSTEM
fi
done
