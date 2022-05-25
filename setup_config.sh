if [ -d "files/config/default" ] then
	sed -i "s@files/config/@files/config/default/@g" src/utils.py
	sed -i "s/\.json/_default.json/" src/utils.py
else 
	sed -i "s/.default//g" src/utils.py
