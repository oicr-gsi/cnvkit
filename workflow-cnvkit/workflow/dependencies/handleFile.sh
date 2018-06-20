#!/bin/bash
wdPath=`pwd`
echo $wdPath
extid=$1
inPath=$wdPath/$2
cd $inPath
model=model-fit; mkdir $model
EXTT= sample

for f in $( ls ); do
	if [[ -d $f ]]; then
		if [[ $f != "model-fit" ]]; then
			cd $inPath
			mv $f $model
		fi
	elif [[ ${f: -20} != ".segmetrics.call.cns" ]]; then	
                         awk -v j=$EXTT ‘{ if (NR>1) {print j”\t”$1"\t”$2"\t”$3"\t”$7"\t”$5 } }’ $f > $EXTT.cnvkit.seg
		fi 
        else 
            mv $f $model
            fi 
	fi
done

cd $model
cd $inPath
tar -zcvf ${model}.tar.gz $model