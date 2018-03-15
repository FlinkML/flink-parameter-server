#!/bin/bash

if [ "$#" -ne 6 ]; then
    echo "Illegal number of parameters. Usage: <accurate result> <prediction> <script location> <word list1>  <word list2>  <word list3>"
    exit 0
fi

acres=$1
prediction=$2
scriptsDir=$3
ARRAY=($4 $5 $6)

sorted=${prediction::-4}-sorted
> $sorted
for j in $(cat $acres | cut -d' ' -f1)
    do cat $prediction | grep "^$j - "  >> $sorted
done

withac=${prediction::-4}-with-ac
> $withac
paste -d "\n" $acres $sorted > $withac

vecd=${prediction::-4}-vecd
correl=${prediction::-4}-correl
> $vecd
> $correl
THIS_DIR="$PWD"
pushd $scriptsDir
python ./vectorize_occurences.py "$THIS_DIR/$withac" "$THIS_DIR/$vecd"
python ./calculate_correlation.py "$THIS_DIR/$vecd" "$THIS_DIR/$correl"
popd

correlfix=${prediction::-4}-correl-fix
> $correlfix
egrep -v nan $correl > $correlfix

avgcorrel=correls
> $avgcorrel
cat $correlfix | tail -n"+2" | awk -F '|' '{p += $2; s += $3; kt += $4; wkt += $5} END{printf("%s %s %s %s\n", p/NR, s/NR, kt/NR, wkt/NR)}' > $avgcorrel

rm -r 1 2 3 > /dev/null 2>&1
mkdir 1 2 3
DIRARRAY=(1 2 3)

for i in {0..2}
    do  for j in $(cat  ${ARRAY[$i]}) 
	    do cat $withac | grep "^$j - " >> "${DIRARRAY[$i]}/$withac"
	done
done

for i in {0..2}
    do cd ${DIRARRAY[$i]}
    THIS_DIR="$PWD"
    pushd $scriptsDir
    python ./vectorize_occurences.py "$THIS_DIR/$withac" "$THIS_DIR/$vecd"
    python ./calculate_correlation.py "$THIS_DIR/$vecd" "$THIS_DIR/$correl"
    popd
    egrep -v nan $correl > $correlfix
    cat $correlfix | tail -n"+2" | awk -F '|' '{p += $2; s += $3; kt += $4; wkt += $5} END{printf("%s %s %s %s\n", p/NR, s/NR, kt/NR, wkt/NR)}' > $avgcorrel
    cd ..
done

for i in $(ls correls */correls); do echo $i: ;cat $i; done
