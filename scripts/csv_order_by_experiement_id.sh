for f in ./*csv
do
	cat $f | sort -n --field-separator=, --key=19 > tmp.csv
	rm $f
	mv tmp.csv $f
done
