#!/bin/bash
wdPath=`pwd`/tmp
model=$wdPath/model-fit;
mkdir -p $model
EXTT=cnvkit
cd $wdPath

for f in $( ls); do
        if [[ -d $f ]]; then #check if directory
                if [[ $f != "model-fit" ]]; then 
                        cd $wdPath
                        mv $f $model
                fi
        elif [[ ${f: -20} = ".segmetrics.call.cns" ]]; then
                        cd $wdPath
                        awk -v j=$EXTT '{ if (NR>1) {print j"\t"$1"\t"$2"\t"$3"\t"$7"\t"$5 } }' $f > $EXTT.seg 
        else
             mv $f $model
        fi
done

cd $wdPath 
tar -zcvf ${model}.tar.gz $model












