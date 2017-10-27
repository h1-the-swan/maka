for (( i = 1980; i < 2000; i++ )); do
	if [[ $(($i % 5)) == 0 ]]; then
		echo "$(date): waiting for jobs to finish before continuing"
		wait
	fi
	echo "$(date): running paperscraper.py for year $i"
	nohup python paperscraper.py $i --outdir paperscrape3/ --debug >& paperscrape3/papers$i.log &
done
wait
echo "$(date): all done"

