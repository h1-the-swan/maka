for (( i = 2000; i < 2010; i++ )); do
	if [[ $(($i % 10)) == 0 ]]; then
		echo "$(date): waiting for jobs to finish before continuing"
		wait
	fi
	echo "$(date): running paperscraper.py for year $i"
	nohup python paperscraper.py $i --outdir paperscrape4/ --offset-thresh 621190 --debug >& paperscrape4/papers$i.log &
done
wait
echo "$(date): all done"

