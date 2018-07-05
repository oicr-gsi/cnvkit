#!/bin/bash
wdPath=`pwd`/tmp
model=$wdPath/model-fit;
mkdir -p $model
cd $wdPath

for f in $( ls ); do
        if [[ -d $f ]]; then #check if directory
                if [[ $f != "model-fit" ]]; then 
                        cd $wdPath
                        mv $f $model
                fi
        else 
            if [[ ${f: -4} != ".seg" ]]; then
                 cd $wdPath  
                 mv $f $model
  		fi 
	fi
done

cd $wdPath 
tar -zcvf ${model}.tar.gz $model












