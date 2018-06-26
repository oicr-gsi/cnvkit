#!/bin/bash
wdPath=`pwd`/tmp
model=$wdPath/model-fit;
mkdir -p $model



for f in ( `ls $wdPath` ); do
        if [[ -d $f ]]; then #check if directory
                if [[ $f != "model-fit" ]]; then 
                        cd $inPath
                        mv $f $model
                fi
        elif [[ ${f: -20} = ".segmetrics.call.cns" ]]; then
                        awk -v j=$EXTT '{ if (NR>1) {print j"\t"$1"\t"$2"\t"$3"\t"$7"\t"$5 } }' $f > $EXTT.cnvkit.seg
               
        else
             mv $f $model
        fi
done

cd $model
tar -zcvf ${model}.tar.gz $model


