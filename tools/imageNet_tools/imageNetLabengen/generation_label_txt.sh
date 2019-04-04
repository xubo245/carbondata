for i in `ls $1`
do
    file_prefix=`echo $i | cut -d . -f1`
    filename=$1"/"$file_prefix".txt"
    touch $filename 
    echo $2 > $filename
done
