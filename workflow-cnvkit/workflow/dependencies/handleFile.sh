#!/bin/bash
wdPath=`pwd`/tmp
model=$wdPath/model-fit;
mkdir -p $model
sampleName=EXTT
cd $wdPath

for f in $( ls ); do
        if [[ -d $f ]]; then #check if directory
                if [[ $f != "model-fit" ]]; then 
                        cd $wdPath
                        mv $f $model
                fi
        else if [[ ${f: -20} != ".segmetrics.call.cns" ]]; then
                 mv $f $model
        fi
done

cd $wdPath 
tar -zcvf ${model}.tar.gz $model












