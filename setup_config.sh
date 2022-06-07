if [ ! $@ ]
then
	PROJ="default"
	RELLOC="default/"
else
	PROJ=$1
	RELLOC=""
fi
echo $PROJ
echo $RELLOC
sed -i "s@files/config.*/@files/config/$RELLOC@g" src/utils.py
sed -i "s/_(?!.*_).*json/$PROJ.json/g" src/utils.py