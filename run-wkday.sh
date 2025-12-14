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
./calc --c conf/config.yml daysum --in /mnt/e/data/raw/$Year/ --out /mnt/e/data/day/$Year/  --repo /mnt/e/data/repo/ --day $Year$i
./calc --c conf/config.yml dayrpt --in /mnt/e/data/day/$Year/ --out /mnt/e/data/rpt/$Year/  --repo /mnt/e/data/repo/ --day $Year$i
./calc --c conf/config.yml daysplit --in /mnt/e/data/day/$Year/ --out /mnt/e/data/split/ --day $Year$i
elif [ $SYSTEM = "Darwin" ] ; then
echo $Year$i
mkdir -p ./datatool/vipdoc/exceptday
cd ./datatool/vipdoc/exceptday
wget https://www.tdx.com.cn/products/autoup/Except$Year.zip && unzip Except$Year.zip && rm -rf Except$Year.zip
cd ../../..
./tdx2db workday  --dbpath tdx.db --wdyear $Year --wdpath ./datatool/vipdoc/exceptday 


./tdx2db cw --cwpath datatool/vipdoc/tdxfin --cwdl true --dbpath tdx.db
./tdx2db base --basepath datatool/vipdoc/base --dbpath tdx.db 
./tdx2db gp --gppath datatool/vipdoc/tdxgp --gpdl true --dbpath tdx.db 
else
    echo  $SYSTEM
fi
done
