for (( i = 1960; i < 2018; i++ )); do
	if [[ $(($i % 5)) == 0 ]]; then
		echo "$(date): waiting for jobs to finish before continuing"
		wait
	fi
	echo "$(date): running paperscraper.py for year $i"
	nohup python paperscraper.py $i --outdir paperscrape4/ --debug >& paperscrape4/papers$i.log &
done
wait
echo "$(date): all done"

